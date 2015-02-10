package colossus
package service

import core._
import testkit._

import akka.pattern.ask

import scala.concurrent.Await
import scala.concurrent.duration._
import java.net.InetSocketAddress

import Callback.Implicits._

import akka.util.ByteString
import RawProtocol._

class AsyncServiceClientSpec extends ColossusSpec {

  def makeServer()(implicit sys: IOSystem): ServerRef = {
    Service.serve[Raw]("redis-test", TEST_PORT){_.handle{_.become{
      case x => x
    }}}
  }

  "AsyncServiceClient" must {
    "send a command" in {
      withIOSystem{ implicit io =>
        withServer(makeServer()) {
          val client = TestClient(io, TEST_PORT)
          Await.result(client.send(ByteString("foo")), 500.milliseconds) must equal(ByteString("foo"))
        }
      }
    }

    "get connection status" in {
      withIOSystem{ implicit io =>
        withServer(makeServer()) {
          val client = TestClient(io, TEST_PORT)
          TestClient.waitForStatus(client, ConnectionStatus.Connected)
        }
      }
    }

    "disconnect from server" in {
      withIOSystem{ implicit io =>
        val server = makeServer()
        withServer(server) {
          val client = TestClient(io, TEST_PORT)
          Await.result(client.connectionStatus, 100.milliseconds) must equal(ConnectionStatus.Connected)
          client.disconnect()
          TestClient.waitForStatus(client, ConnectionStatus.NotConnected)
          TestUtil.expectServerConnections(server, 0)
        }
      }
    }

    "shutdown when connection is unbound" in {
      var client: Option[AsyncServiceClient[ByteString, ByteString]] = None
      withIOSystem{ implicit io =>
        withServer(makeServer()) {
          client = Some(TestClient(io, TEST_PORT, connectionAttempts = PollingDuration.NoRetry))
          Await.result(client.get.send(ByteString("foo")), 500.milliseconds) must equal(ByteString("foo"))
        }
        TestClient.waitForStatus(client.get, ConnectionStatus.NotConnected)
      }
    }

  }
}

