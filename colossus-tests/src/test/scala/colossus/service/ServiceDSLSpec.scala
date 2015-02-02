package colossus

import testkit._

import akka.actor._
import akka.testkit.TestProbe
import akka.util.ByteString

import scala.concurrent.duration._

import protocols.telnet._
import service._
import Callback.Implicits._

import RawProtocol.{RawCodec, Raw}

class ErrorTestDSL(probe: ActorRef) extends CodecProvider[Raw] {

    def provideCodec() = RawCodec

    def errorResponse(request: ByteString, reason: Throwable) = {
      probe ! reason
      ByteString(s"Error (${reason.getClass.getName}): ${reason.getMessage}")
    }
  
}



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
        probe.expectMsg(250.milliseconds, "PONG")
        probe.expectMsg(250.milliseconds, "PONG")
      }
    }

    "throw UnhandledRequestException on unhandled request" in {
      val probe = TestProbe() 
      implicit val provider = new ErrorTestDSL(probe.ref)
      withIOSystem{ implicit system => 
        val server = Service.become[Raw]("test", TEST_PORT) { 
          case any if (false) => ByteString("WAT")
        }
        withServer(server) {
          val client = TestClient(system, TEST_PORT)
          client.send(ByteString("hello"))
          probe.expectMsgType[UnhandledRequestException]
        }
      }
    }

    "receive connection messages" in {
      val probe = TestProbe()
      withIOSystem{ implicit system =>
        val server = Service.serve[Raw]("test", TEST_PORT) { context =>
          context.handle{ connection =>
            connection.receive{
              case "PING" => {
                probe.ref ! "PONG"
              }
            }
            connection.become{
              case x if (x == ByteString("PING")) => {
                context.worker.worker ! core.WorkerCommand.Message(connection.connectionId, "PING")
                Callback.successful(ByteString("WHATEVER"))
              }
            }
          }
        }
        withServer(server) {
          val client = TestClient(system, TEST_PORT)
          client.send(ByteString("PING"))
          probe.expectMsg(250.milliseconds, "PONG")
        }
      }
    }

  }
}


