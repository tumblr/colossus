package colossus
package service

import core._
import testkit._

import akka.actor._
import akka.pattern.ask

import scala.concurrent.Await
import scala.concurrent.duration._
import java.net.InetSocketAddress

import protocols.redis._
import metrics._

object RedisTest {

  def server(port: Int)(implicit sys: IOSystem): ServerRef = {
    implicit val s: ActorSystem = sys.actorSystem
    import sys.actorSystem.dispatcher
    import service._
    import UnifiedProtocol._
    Service.serve[Redis]("redis-test", port){_.handle{_.become{
      case cmd if (cmd.command == CMD_GET) => StatusReply("echo")
    }}}
  }
}


class ClientSpec extends ColossusSpec {

  def makeClient: (ServerRef, AsyncServiceClient[Command, Reply]) = {
    implicit val io = IOSystem("test", 2)
    val server = RedisTest.server(TEST_PORT)
    waitForServer(server)
    val config = ClientConfig(
      address = new InetSocketAddress("localhost", TEST_PORT),
      requestTimeout = Duration.Inf,
      name = MetricAddress.Root / "test-client"
    )
    val client = AsyncServiceClient(config, new RedisClientCodec)
    (server, client)
  }

  def waitForClient(client: ActorRef) {
    var attempts = 0
    while (attempts < 5 && Await.result((client ? AsyncServiceClient.GetConnectionStatus), 50.milliseconds) != ConnectionStatus.Connected) {
      attempts += 1
    }
    if (attempts >= 5) {
      throw new Exception("timed out waiting for client to connect")
    }
  }
      

  "AsyncServiceClient" must {
    "send a command" in {
      val (server, client) = makeClient
      Await.result(client.send(Command("GET", "foo")), 5000.milliseconds) must equal(StatusReply("echo"))
      end(server)
    }


    "get connection status" in {
      val (server, client) = makeClient
      Await.result(client.connectionStatus, 100.milliseconds) must equal(ConnectionStatus.Connecting)
      Thread.sleep(100)
      Await.result(client.connectionStatus, 100.milliseconds) must equal(ConnectionStatus.Connected)
      end(server)
    }

    "disconnect from server" in {
      val (server, client) = makeClient
      Thread.sleep(100)
      Await.result(client.connectionStatus, 100.milliseconds) must equal(ConnectionStatus.Connected)
      client.disconnect()
      intercept[Exception] {
        Await.result(client.connectionStatus, 100.milliseconds) must equal(ConnectionStatus.Connected)
      }

      end(server)
    }

  }
}

