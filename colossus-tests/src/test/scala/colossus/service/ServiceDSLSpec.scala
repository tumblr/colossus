package colossus
package service

import core.ProxyActor
import testkit._

import akka.actor._
import akka.testkit.TestProbe
import akka.util.ByteString

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

import Callback.Implicits._

import RawProtocol._
import server._

class ServiceDSLSpec extends ColossusSpec {

  "Service DSL" must {

    /**
     * TODO: move to Server DSL tests
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
        server.initializerBroadcast("PING")
        probe.expectMsg(250.milliseconds, "PONG")
        probe.expectMsg(250.milliseconds, "PONG")
      }
    }
    */

    /*
     * TODO: rewrite this to test DSLhandler instead

    "throw UnhandledRequestException on unhandled request" in {
      val probe = TestProbe()
      implicit val provider = new ErrorTestDSL(probe.ref)
      withIOSystem{ implicit system =>
        val server = Server.basic[Raw]("test", TEST_PORT) {
          case any if (false) => ByteString("WAT")
        }
        withServer(server) {
          val client = TestClient(system, TEST_PORT)
          client.send(ByteString("hello"))
          probe.expectMsgType[UnhandledRequestException]
        }
      }
    }
    */


    "receive connection messages" in {
      val probe = TestProbe()
      withIOSystem{ implicit system =>
        val server = RawServer.basic("test", TEST_PORT, new RequestHandler(_) with ProxyActor {
            def shutdownRequest() {
              shutdown()
            }

            override def receive = {
              case "PING" => {
                probe.ref ! "PONG"
              }
            }
            def handle = {
              case x if (x == ByteString("PING")) => {
                self ! "PING"
                Callback.successful(ByteString("WHATEVER"))
              }
            }
          }
        )
        withServer(server) {
          val client = TestClient(system, TEST_PORT)
          client.send(ByteString("PING"))
          probe.expectMsg(250.milliseconds, "PONG")
        }
      }
    }

    "override error handler" in {
      withIOSystem{ implicit system =>
        val server = RawServer.basic("test", TEST_PORT, new RequestHandler(_) {
            override def onError = {
              case error => ByteString("OVERRIDE")
            }
            def handle = {
              case x if (false) => ByteString("NOPE")
            }
          }
        )
        withServer(server) {
          val client = TestClient(system, TEST_PORT)
          Await.result(client.send(ByteString("TEST")), 1.second).utf8String must equal("OVERRIDE")
        }
      }
    }

    "be able to create two clients of differing codecs" in {
      withIOSystem{ implicit sys =>
        import protocols.http._
        import protocols.memcache._
        //this test passes if it compiles
        val s = Http.futureClient("localhost", TEST_PORT, 1.second)
        val t = Memcache.futureClient("localhost", TEST_PORT, 1.second)
      }
    }

    "be able to lift a sender to a type-specific client" in {
      withIOSystem{ implicit sys =>
        import protocols.http._

        val s = Http.futureFactory("localhost", TEST_PORT, 1.second)
        val t = Http.futureClient(s)
        val q : HttpClient[Future] = t
      }
    }
  }
}


