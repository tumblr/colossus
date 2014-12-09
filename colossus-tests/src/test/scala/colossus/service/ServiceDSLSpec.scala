package colossus

import testkit._

import akka.actor._
import akka.testkit.TestProbe

import scala.concurrent.duration._

import protocols.telnet._
import service._
import Completion.Implicits._


class ServiceDSLSpec extends ColossusSpec {

  "Service DSL" must {
    "receive delegator messages" in {
      withIOSystem{implicit system =>
        val probe = TestProbe()
        val server = Service.serve[Telnet]("test", 12345){context =>
          context.receive{
            case "PING" => probe.ref ! "PONG"
          }
          context.handle{_.become{
            case _ => TelnetReply("meeh")
          }}
        }
        server.delegatorBroadcast("PING")
        probe.expectMsg(50.milliseconds, "PONG")
        probe.expectMsg(50.milliseconds, "PONG")
      }
    }
  }
}


