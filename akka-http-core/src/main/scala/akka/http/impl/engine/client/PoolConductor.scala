/**
 * Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.http.impl.engine.client

import language.existentials
import scala.annotation.tailrec
import scala.collection.immutable
import akka.event.LoggingAdapter
import akka.stream.scaladsl._
import akka.stream._
import akka.http.scaladsl.util.FastFuture
import akka.http.scaladsl.model.HttpMethod
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler

private object PoolConductor {
  import PoolFlow.RequestContext
  import PoolSlot.{ RawSlotEvent, SlotEvent }

  case class Ports(
    requestIn:   Inlet[RequestContext],
    slotEventIn: Inlet[RawSlotEvent],
    slotOuts:    immutable.Seq[Outlet[SlotCommand]]) extends Shape {

    override val inlets = requestIn :: slotEventIn :: Nil
    override def outlets = slotOuts

    override def deepCopy(): Shape =
      Ports(
        requestIn.carbonCopy(),
        slotEventIn.carbonCopy(),
        slotOuts.map(_.carbonCopy()))

    override def copyFromPorts(inlets: immutable.Seq[Inlet[_]], outlets: immutable.Seq[Outlet[_]]): Shape =
      Ports(
        inlets.head.asInstanceOf[Inlet[RequestContext]],
        inlets.last.asInstanceOf[Inlet[RawSlotEvent]],
        outlets.asInstanceOf[immutable.Seq[Outlet[SlotCommand]]])
  }

  final case class PoolSlotsSetting(minSlots: Int, maxSlots: Int) {
    require(minSlots <= maxSlots, "min-connections must be <= max-connections")
  }

  /*
    Stream Setup
    ============
                                                                                                  Slot-
    Request-   +-----------+     +-----------+    Switch-    +-------------+     +-----------+    Command
    Context    |   retry   |     |   slot-   |    Command    |   doubler   |     |   route   +-------------->
    +--------->|   Merge   +---->| Selector  +-------------->| (MapConcat) +---->|  (Flexi   +-------------->
               |           |     |           |               |             |     |   Route)  +-------------->
               +----+------+     +-----+-----+               +-------------+     +-----------+       to slots
                    ^                  ^
                    |                  | SlotEvent
                    |             +----+----+
                    |             | flatten | mapAsync
                    |             +----+----+
                    |                  | RawSlotEvent
                    | Request-         |
                    | Context     +---------+
                    +-------------+  retry  |<-------- RawSlotEvent (from slotEventMerge)
                                  |  Split  |
                                  +---------+

  */
  def apply(slotSettings: PoolSlotsSetting, pipeliningLimit: Int, log: LoggingAdapter): Graph[Ports, Any] =
    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._

      val retryMerge = b.add(MergePreferred[RequestContext](1, eagerComplete = true))
      val slotSelector = b.add(new SlotSelector(slotSettings, pipeliningLimit, log))
      val route = b.add(new Route(slotSettings.maxSlots))
      val retrySplit = b.add(Broadcast[RawSlotEvent](2))
      val flatten = Flow[RawSlotEvent].mapAsyncUnordered(slotSettings.maxSlots * pipeliningLimit) {
        case x: SlotEvent.Disconnected                ⇒ log.debug("flatten: slotevent.disconnected {}", x); FastFuture.successful(x)
        case SlotEvent.RequestCompletedFuture(future) ⇒ log.debug("flatten: requestcompleted: {}", future); future
        case x: SlotEvent.ConnectedEagerly            ⇒ FastFuture.successful(x)
        case x                                        ⇒ log.error("flatten: WTF {}", x); throw new IllegalStateException("Unexpected " + x)
      }

      retryMerge.out ~> /*buffer ~> */slotSelector.in0
      slotSelector.out ~> route.in
      retrySplit.out(0).filter(!_.isInstanceOf[SlotEvent.RetryRequest]) ~> Flow[RawSlotEvent].buffer(10, OverflowStrategy.backpressure) ~> flatten ~> slotSelector.in1
      retrySplit.out(1).collect { case SlotEvent.RetryRequest(r) ⇒ r } ~> Flow[RequestContext].buffer(10, OverflowStrategy.backpressure) ~> retryMerge.preferred

      Ports(retryMerge.in(0), retrySplit.in, route.outArray.toList)
    }

  sealed trait SlotCommand
  final case class DispatchCommand(rc: RequestContext) extends SlotCommand
  final case object ConnectEagerlyCommand extends SlotCommand

  final case class SwitchSlotCommand(cmd: SlotCommand, slotIx: Int)

  // the SlotSelector keeps the state of all slots as instances of this ADT
  private sealed trait SlotState

  // the connection of the respective slot is not connected
  private case object Unconnected extends SlotState

  // the connection of the respective slot is connected with no requests currently in flight
  private case object Idle extends SlotState

  // the connection of the respective slot has a number of requests in flight and all of them
  // are idempotent which allows more requests to be pipelined onto the connection if required
  private final case class Loaded(openIdempotentRequests: Int) extends SlotState { require(openIdempotentRequests > 0) }

  // the connection of the respective slot has a number of requests in flight and the
  // last one of these is not idempotent which blocks the connection for more pipelined requests
//  private case class Busy(openRequests: Int) extends SlotState { require(openRequests > 0) }
  private object Busy extends SlotState

  private case class Closing(openRequests: Int) extends SlotState

  private class SlotSelector(slotSettings: PoolSlotsSetting, pipeliningLimit: Int, log: LoggingAdapter)
    extends GraphStage[FanInShape2[RequestContext, SlotEvent, SwitchSlotCommand]] {

    private val ctxIn = Inlet[RequestContext]("requestContext")
    private val slotIn = Inlet[SlotEvent]("slotEvents")
    private val out = Outlet[SwitchSlotCommand]("slotCommand")

    override def initialAttributes = Attributes.name("SlotSelector")

    override val shape = new FanInShape2(ctxIn, slotIn, out)

    override def createLogic(effectiveAttributes: Attributes) = new GraphStageLogic(shape) {
      val slotStates = Array.fill[SlotState](slotSettings.maxSlots)(Unconnected)
      var nextSlot = 0
      var stashed: RequestContext = _

      setHandler(ctxIn, new InHandler {
        override def onPush(): Unit = {
          val ctx: RequestContext = grab(ctxIn)
          log.debug("SlotSelector new request {} nextSlot {}", ctx, nextSlot)
          // if we have a non-idempotent request, pipelining is enabled and the slot is currently
          // processing requests then try get a new slot suitable for idempotent requests
          val slot =
            if (!ctx.request.method.isIdempotent && pipeliningLimit > 1 /*&& (nextSlot == -1 || slotStates(nextSlot).isInstanceOf[Loaded])*/) bestSlotNonIdempotent()
            else nextSlot

          if (slot == -1) {
            nextSlot = -1
            if (stashed != null) log.error("stashed is not null!? {}" + slotStates)
            log.debug("SlotSelector stashing {}", ctx)
            stashed = ctx
          } else {
            log.debug("{} before dispatch {}", slot, slotStates(slot))
            slotStates(slot) = slotStateAfterDispatch(slotStates(slot), ctx.request.method)
            log.debug("{} after dispatch {}", slot, slotStates(slot))
            nextSlot = bestSlot()
            emit(out, SwitchSlotCommand(DispatchCommand(ctx), slot), tryPullCtx)
          }
        }
      })

      setHandler(slotIn, new InHandler {
        override def onPush(): Unit = {
          grab(slotIn) match {
            case SlotEvent.RequestCompleted(slotIx, willClose) ⇒
              log.debug("SlotSelector-{} request completed willClose {}", slotIx, willClose)
              log.debug("SlotSelector-{} before completed {}", slotIx, slotStates(slotIx))
              slotStates(slotIx) = slotStateAfterRequestCompleted(slotStates(slotIx), willClose)
              log.debug("SlotSelector-{} after completed {}", slotIx, slotStates(slotIx))
            case SlotEvent.Disconnected(slotIx, failed) ⇒
              log.debug("SlotSelector-{} slot disconnected {} failures", slotIx, failed)
              log.debug("SlotSelector-{} before disconnect {}", slotIx, slotStates(slotIx))
              slotStates(slotIx) = slotStateAfterDisconnect(slotStates(slotIx), failed)
              log.debug("SlotSelector-{} after disconnect {}", slotIx, slotStates(slotIx))
              reconnectIfNeeded()
            case SlotEvent.ConnectedEagerly(slotIx) ⇒
              log.debug("SlotSelector-{} connected eagerly", slotIx)
            // do nothing ...
          }
          log.debug("SlotSelector {}", hasBeenPulled(slotIn))
          pull(slotIn)
          log.debug("SlotSelector pulled {} available {}", hasBeenPulled(slotIn), isAvailable(slotIn))
          val wasBlocked = nextSlot == -1
          nextSlot = if (stashed == null) bestSlot() else bestSlotNonIdempotent()
          val nowUnblocked = nextSlot != -1
          log.debug("wasBlocked {} nowUnblocked {} stashed {}", wasBlocked, nowUnblocked, stashed)
          if (wasBlocked && nowUnblocked) {
            if (stashed != null) {
              val slot = nextSlot
              log.debug("{} Unstashing {}", slot, stashed)
              slotStates(slot) = slotStateAfterDispatch(slotStates(slot), stashed.request.method)
              log.debug("{} after unstashing {}", slot, slotStates(slot))
              nextSlot = bestSlot()
              val sendme = stashed
              stashed = null
              emit(out, SwitchSlotCommand(DispatchCommand(sendme), slot), tryPullCtx)
            } else tryPullCtx()
          }
        }
      })

      setHandler(out, eagerTerminateOutput)

      val tryPullCtx = () ⇒ if (nextSlot != -1 && !hasBeenPulled(ctxIn)) pull(ctxIn)

      override def preStart(): Unit = {
        pull(ctxIn)
        pull(slotIn)

        // eagerly start at least slotSettings.minSlots connections
        (0 until slotSettings.minSlots).foreach { connect }
      }

      def connect(slotIx: Int): Unit = {
        emit(out, SwitchSlotCommand(ConnectEagerlyCommand, slotIx))
        slotStates(slotIx) = Idle
      }

      private def reconnectIfNeeded(): Unit =
        if (slotStates.count(_ != Unconnected) < slotSettings.minSlots) {
          connect(slotStates.indexWhere(_ == Unconnected))
        }

      def slotStateAfterDispatch(slotState: SlotState, method: HttpMethod): SlotState =
        slotState match {
          case Unconnected | Idle               ⇒ if (method.isIdempotent) Loaded(1) else Busy
          case Loaded(n) if method.isIdempotent ⇒ Loaded(n + 1)
          case Loaded(_)          ⇒ throw new IllegalStateException("non-idempotent Request scheduled onto Loaded connection?")
          case Busy               ⇒ throw new IllegalStateException("Request scheduled onto busy connection?")
          case Closing(_)         ⇒ throw new IllegalStateException("Request scheduled onto closing connection?")
        }

      def slotStateAfterRequestCompleted(slotState: SlotState, willClose: Boolean): SlotState =
        slotState match {
          case Loaded(1)  ⇒ if (willClose) Closing(0) else Idle
          case Loaded(n)  ⇒ if (willClose) Closing(n - 1) else Loaded(n - 1)
          case Busy       ⇒ if (willClose) Closing(0) else Idle
          case Closing(1) ⇒ Unconnected
          case Closing(n) ⇒ Closing(n - 1)
          case _          ⇒ throw new IllegalStateException(s"RequestCompleted on $slotState connection?")
        }

      def slotStateAfterDisconnect(slotState: SlotState, failed: Int): SlotState =
        slotState match {
          case Idle if failed == 0       ⇒ Unconnected
          case Loaded(n) if n > failed   ⇒ Closing(n - failed)
          case Loaded(n) if n == failed  ⇒ Unconnected
          case Busy if failed == 1       ⇒ Unconnected
          case Busy                      ⇒ Closing(1)
          case Closing(n) if n > failed  ⇒ Closing(n - failed)
          case Closing(_)                ⇒ Unconnected
          case _                         ⇒ throw new IllegalStateException(s"Disconnect(_, $failed) on $slotState connection?")
        }

      /**
       * Implements the following Connection Slot selection strategy
       *  - Select the first idle connection in the pool, if there is one.
       *  - If none is idle select the first unconnected connection, if there is one.
       *  - If all are loaded select the connection with the least open requests (< pipeliningLimit)
       *    that only has requests with idempotent methods scheduled to it, if there is one.
       *  - Otherwise return -1 (which applies back-pressure to the request source)
       *
       *  See http://tools.ietf.org/html/rfc7230#section-6.3.2 for more info on HTTP pipelining.
       */
      @tailrec def bestSlot(ix: Int = 0, bestIx: Int = -1, bestState: SlotState = Busy): Int =
        if (ix < slotStates.length) {
          val pl = pipeliningLimit
          slotStates(ix) → bestState match {
            case (Idle, _)                           ⇒ ix
            case (Unconnected, Loaded(_) | Busy)     ⇒ bestSlot(ix + 1, ix, Unconnected)
            case (x @ Loaded(a), Loaded(b)) if a < b ⇒ bestSlot(ix + 1, ix, x)
            case (x @ Loaded(a), Busy) if a < pl     ⇒ bestSlot(ix + 1, ix, x)
            case _                                   ⇒ bestSlot(ix + 1, bestIx, bestState)
          }
        } else bestIx

      @tailrec def bestSlotNonIdempotent(ix: Int = 0, bestIx: Int = -1, bestState: SlotState = Busy): Int =
        if (ix < slotStates.length) {
          val pl = pipeliningLimit
          slotStates(ix) → bestState match {
            case (Idle, _)                           ⇒ ix
            case (Unconnected, Loaded(_) | Busy)     ⇒ bestSlotNonIdempotent(ix + 1, ix, Unconnected)
            case _                                   ⇒ bestSlotNonIdempotent(ix + 1, bestIx, bestState)
          }
        } else bestIx
    }
  }

  private class Route(slotCount: Int) extends GraphStage[UniformFanOutShape[SwitchSlotCommand, SlotCommand]] {

    override def initialAttributes = Attributes.name("PoolConductor.Route")

    override val shape = new UniformFanOutShape[SwitchSlotCommand, SlotCommand](slotCount)

    override def createLogic(effectiveAttributes: Attributes) = new GraphStageLogic(shape) {
      shape.outArray foreach { setHandler(_, ignoreTerminateOutput) }

      val in = shape.in
      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val switchCommand = grab(in)
          //emit(shape.outArray(switchCommand.slotIx), switchCommand.cmd, pullIn)
          emit(shape.outArray(switchCommand.slotIx), switchCommand.cmd, () => {
            materializer.asInstanceOf[ActorMaterializer].logger.debug("Route pull(in)")
            pull(in)
          })
        }
      })
      val pullIn = () ⇒ pull(in)

      override def preStart(): Unit = pullIn()
    }
  }
}
