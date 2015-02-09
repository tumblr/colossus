package colossus
package service

import testkit._
import core._
import Callback.Implicits._

import scala.concurrent.duration._
import akka.util.ByteString
import java.net.InetSocketAddress

import protocols.redis._
import scala.concurrent.Await

class ServiceServerSpec extends ColossusSpec {

  "ServiceServer" must {
    "timeout request that takes too long" in {
      val serverSettings = ServerSettings (
        port = TEST_PORT,
        maxIdleTime = Duration.Inf
      )

      val serviceConfig = ServiceConfig (
        name = "/timeout-test",
        requestTimeout = 50.milliseconds
      )
      withIOSystem{implicit io => 
        val server = Service.serve[Redis](serverSettings, serviceConfig) { context => 
          context.handle{ connection =>
            import connection.callbackExecutor
            connection.become{
              case req => Callback.schedule(500.milliseconds)(Callback.successful(StatusReply("HEllo")))
            }
          }
        }
        withServer(server) {
          val clientConfig = ClientConfig(
            name = "/test-client",
            address = new InetSocketAddress("localhost", TEST_PORT),
            requestTimeout = Duration.Inf,
            connectionAttempts = PollingDuration.NoRetry
          )
          val client = AsyncServiceClient(clientConfig, new RedisClientCodec)
          Await.result(client.send(Commands.Get(ByteString("foo"))), 1.second) match {
            case e: ErrorReply => {}
            case other => throw new Exception(s"Non-error reply: $other")
          }
        }
      }
    }

    "graceful disconnect" in {
      withIOSystem{implicit io => 
        val server = Service.serve[Redis]("test", TEST_PORT){_.handle{con => con.become{
          case x if (x.command == "DIE") => {
            con.gracefulDisconnect()
            StatusReply("BYE")
          }
          case other => {
            import con.callbackExecutor
            Callback.schedule(100.milliseconds)(StatusReply("FOO"))
          }
        }}}
        withServer(server) {
          val client = AsyncServiceClient[Redis]("localhost", TEST_PORT)
          val r1 = client.send(Command("TEST"))
          val r2 = client.send(Command("DIE"))
          Await.result(r1, 1.second) must equal(StatusReply("FOO"))
          Await.result(r2, 1.second) must equal(StatusReply("BYE"))
        }
      }
    }

  }

  "Streaming Service" must {

    import protocols.http._

    "Serve a basic response as a stream" taggedAs(org.scalatest.Tag("Test")) in {
      withIOSystem{implicit io =>
        val server = Service.become[StreamingHttp]("stream-test", TEST_PORT) {
          case req => StreamingHttpResponse.fromStatic(req.ok("Hello World"))
        }
        withServer(server) { 
          val client = AsyncServiceClient[Http]("localhost", TEST_PORT)
          Await.result(client.send(HttpRequest.get("/")), 1.second).data must equal(ByteString("Hello World"))
        }
      }
    }
  }

      

}
