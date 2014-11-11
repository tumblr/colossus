package colossus
package testkit

import scala.concurrent.duration._

import protocols.redis._
import service._

import org.scalatest.exceptions.TestFailedException

object TestService {
  def apply()(implicit io: IOSystem) = Service.serve[Redis]("localhost", 3535){context => 
    context.handle{connection =>
      import connection._
      become{
        case c: Command if (c.command == "GET") => StatusReply("OK")
        case c: Command if (c.command == "DELAY") => Callback.schedule(1.second)(Callback.successful(StatusReply("OK")))
      }
    }
  }
}


class ServiceSpecSpec extends ServiceSpec[Redis] {

  def service = TestService()
  val requestTimeout = 500.milliseconds

  "test service" must {
    "expect a response" in {
      expectResponse(Command("GET", "wahtever"), StatusReply("OK"))
    }

    "expectResponse properly times out" in {
      intercept[TestFailedException]{
        expectResponse(Command("DELAY"), StatusReply("OK"))
      }
    }      

    "expect a response of type" in {
      expectResponseType[ErrorReply](Command("ASDF"))
    }

    
  }
}
