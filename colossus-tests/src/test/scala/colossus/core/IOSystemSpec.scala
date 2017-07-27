package colossus

import testkit._
import core._

import akka.actor._
import akka.testkit.TestProbe

import scala.concurrent.duration._
import java.net.InetSocketAddress

import service._
import Callback.Implicits._

import RawProtocol._

class IOSystemSpec extends ColossusSpec {

  "IOSystem" must {
    "connect client handler using connect method" in {
      withIOSystem{implicit sys =>
        val probe = TestProbe()
        class MyHandler(c: Context) extends NoopHandler(c) {
          def connectionFailed(){}
          override def connected(w: WriteEndpoint) {
            probe.ref ! "CONNECTED"
          }
        }

        val server = RawServer.basic("test", 15151){case x => x}
        waitForServer(server)

        sys.connect(new InetSocketAddress("localhost", 15151), new MyHandler(_))
        probe.expectMsg(200.milliseconds, "CONNECTED")

      }
    }
  }
}
