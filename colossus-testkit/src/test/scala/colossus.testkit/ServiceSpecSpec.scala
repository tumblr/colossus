package colossus

import scala.concurrent.duration._
import protocols.redis._
import protocols.redis.server._
import service._
import org.scalatest.exceptions.TestFailedException
import Callback.Implicits._
import colossus.testkit.ServiceSpec

object TestService {
  def apply()(implicit io: IOSystem) =
    RedisServer.basic(
      "localhost",
      3535,
      serverContext => new RequestHandler(serverContext) {
        def handle = {
          case c: Command if (c.command == "GET") => StatusReply("OK")
          case c: Command if (c.command == "DELAY") =>
            Callback.schedule(200.milliseconds)(Callback.successful(StatusReply("OK")))
        }
      }
    )
}

class ServiceSpecSpec extends ServiceSpec[Redis] {

  def service        = TestService()
  val requestTimeout = 5.seconds

  "test service" must {
    "expect a response" in {
      expectResponse(Command("GET", "wahtever"), StatusReply("OK"))
    }

    "expect a response of type" in {
      expectResponseType[ErrorReply](Command("ASDF"))
    }

  }
}

class ServiceSpecTimeoutSpec extends ServiceSpec[Redis] {

  def service = TestService()

  val requestTimeout = 100.milliseconds

  "test service" must {
    "expectResponse properly times out" in {
      intercept[TestFailedException] {
        expectResponse(Command("DELAY"), StatusReply("OK"))
      }
    }
  }
}
