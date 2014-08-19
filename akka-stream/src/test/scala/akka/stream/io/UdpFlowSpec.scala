/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.io

import java.net.InetSocketAddress
import java.nio.channels.DatagramChannel

import akka.io.IO
import akka.stream.scaladsl.Flow
import akka.stream.testkit.AkkaSpec
import akka.stream.{FlowMaterializer, MaterializerSettings}
import akka.testkit.TestProbe
import akka.util.ByteString

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


class UdpFlowSpec extends AkkaSpec {
  val settings = MaterializerSettings(
    initialInputBufferSize = 4,
    maximumInputBufferSize = 4,
    initialFanOutBufferSize = 2,
    maxFanOutBufferSize = 2,
    dispatcher = "akka.test.stream-dispatcher")

  val materializer = FlowMaterializer(settings)

  def temporaryServerAddress: InetSocketAddress = {
    val serverSocket = DatagramChannel.open().socket()
    serverSocket.bind(new InetSocketAddress("127.0.0.1", 0))
    val address = new InetSocketAddress("127.0.0.1", serverSocket.getLocalPort)
    serverSocket.close()
    address
  }

  def connect: StreamUdp.UdpConnection = {
    val connectProbe = TestProbe()
    connectProbe.send(IO(StreamUdp), StreamUdp.Bind(settings, new InetSocketAddress("127.0.0.1", 0)))
    connectProbe.expectMsgType[StreamUdp.UdpConnection]
  }

  def bind(serverAddress: InetSocketAddress = temporaryServerAddress): StreamUdp.UdpConnection = {
    val bindProbe = TestProbe()
    bindProbe.send(IO(StreamUdp), StreamUdp.Bind(settings, serverAddress))
    bindProbe.expectMsgType[StreamUdp.UdpConnection]
  }

  def echoServer(serverAddress: InetSocketAddress = temporaryServerAddress) = {
    val server = bind(serverAddress)
    server.inputStream.subscribe(server.outputStream)
  }

  "UDP listen stream" must {
    "be able to implement echo" in {
      val serverAddress = temporaryServerAddress
      val server = echoServer(serverAddress)
      val conn = connect

      val testInput = Iterator.fill(20)(StreamUdp.UdpPacket(ByteString('A'), serverAddress))
      val expectedOutput = ByteString(Array.fill[Byte](10)('A'))

      // send 20 but just read 10 as UDP is unreliable
      Flow(testInput).toPublisher(materializer).subscribe(conn.outputStream)
      val resultFuture: Future[ByteString] = Flow(conn.inputStream).take(10).
        fold(ByteString.empty)((acc, in) ⇒ acc ++ in.content).toFuture(materializer)

      Await.result(resultFuture, 3.seconds) should be(expectedOutput)
    }
  }
}
