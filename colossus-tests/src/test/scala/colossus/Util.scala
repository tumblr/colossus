package colossus

import testkit._

import core._

import java.net.InetSocketAddress
import service.{Codec, AsyncServiceClient, ClientConfig}

import akka.actor._
import akka.testkit.TestProbe

import akka.util.ByteString
import java.net.{SocketException, Socket}

import scala.concurrent.Await
import scala.concurrent.duration._


class EchoHandler extends BasicSyncHandler {
  def receivedData(data: DataBuffer){ 
    endpoint.write(data)
  }
}


object RawCodec extends Codec.ClientCodec[ByteString, ByteString] {
  def decode(data: DataBuffer) = Some(ByteString(data.takeAll))
  def encode(raw: ByteString) = DataBuffer(raw)
  def reset(){}
}

object TestClient {

  def apply(io: IOSystem, port: Int, waitForConnected: Boolean = true): AsyncServiceClient[ByteString, ByteString] = {
    val config = ClientConfig(
      name = "/test",
      requestTimeout = 100.milliseconds,
      address = new InetSocketAddress("localhost", port),
      pendingBufferSize = 0,
      failFast = true
    )
    val client = AsyncServiceClient(config, RawCodec)(io)
    if (waitForConnected) {
      var tries = 10
      val sleepMillis = 50
      while (Await.result(client.connectionStatus, 50.milliseconds) != ConnectionStatus.Connected) {
        Thread.sleep(sleepMillis)
        tries -= 1
        if (tries == 0) {
          throw new Exception("Test client failed to connect")
        }
      }
    }
    client
  }
}
