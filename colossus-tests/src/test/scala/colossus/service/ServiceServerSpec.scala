package colossus

import testkit._
import core._
import service._
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

  }

}
