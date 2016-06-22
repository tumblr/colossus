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

class FutureClientSpec extends ColossusSpec {

  def makeServer()(implicit sys: IOSystem): ServerRef = {
    Service.basic[Raw]("future-client-test", TEST_PORT){case x => x}
  }

  "FutureClient" must {
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
          TestUtil.expectServerConnections(server, 0)
          TestClient.waitForStatus(client, ConnectionStatus.NotConnected)
        }
      }
    }

    "properly fail requests after disconnected" in {
      withIOSystem{ implicit io =>
        withServer(makeServer()) {
          val client = TestClient(io, TEST_PORT)
          client.disconnect()
          val toolate = client.send(ByteString("bar"))
          intercept[ServiceClientException] {
            Await.result(toolate, 500.milliseconds)
          }
        }
      }

    }

    "allow graceful disconnect" in {
      withIOSystem{ implicit io =>
        withServer(makeServer()) {
          val client = TestClient(io, TEST_PORT)
          val future = client.send(ByteString("foo"))
          client.disconnect()
          val toolate = client.send(ByteString("bar"))
          Await.result(future, 500.milliseconds) must equal(ByteString("foo"))
          intercept[ServiceClientException] {
            Await.result(toolate, 500.milliseconds)
          }
        }
      }
    }

    "properly buffer messages before workeritem bound" in {
      withIOSystem{ implicit io =>
        val client = TestClient(io, TEST_PORT, waitForConnected = false)
        intercept[NotConnectedException] {
          Await.result(client.send(ByteString("foo")), 500.milliseconds)
        }
      }
    }


    "shutdown when connection is unbound" in {
      var client: Option[FutureClient[Raw]] = None
      withIOSystem{ implicit io =>
        withServer(makeServer()) {
          client = Some(TestClient(io, TEST_PORT, connectRetry = NoRetry))
          Await.result(client.get.send(ByteString("foo")), 500.milliseconds) must equal(ByteString("foo"))
        }
        TestClient.waitForStatus(client.get, ConnectionStatus.NotConnected)
      }
    }

  }
}

