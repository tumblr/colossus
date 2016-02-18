package colossus

import testkit._
import core._

import akka.actor._
import akka.testkit.TestProbe

import scala.concurrent.duration._
import java.net.InetSocketAddress

import protocols.telnet._
import service._
import Callback.Implicits._


class IOSystemSpec extends ColossusSpec {

  "IOSystem" must {
    "connect client handler using connect method" in {
      withIOSystem{implicit sys =>
        val probe = TestProbe()
        class MyHandler(c: Context) extends BasicSyncHandler(c) with  ClientConnectionHandler {
          def connectionFailed(){}
          def receivedData(data: DataBuffer){}
          override def connected(w: WriteEndpoint) {
            probe.ref ! "CONNECTED"
          }
        }

        val server = Service.basic[Telnet]("test", 15151){case _ => TelnetReply("ASDF")}
        waitForServer(server)

        sys.connect(new InetSocketAddress("localhost", 15151), new MyHandler(_))
        probe.expectMsg(200.milliseconds, "CONNECTED")

      }
    }
  }
}
