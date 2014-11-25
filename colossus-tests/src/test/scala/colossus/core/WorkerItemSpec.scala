package colossus
package task

import testkit._

import core._

import akka.actor._
import akka.testkit.TestProbe

import scala.concurrent.duration._

class WorkerItemSpec extends ColossusSpec {

  "A WorkerItem" must {
    "bind to a worker" in {
      withIOSystem{io => 
        val probe = TestProbe()
        class MyItem extends BindableWorkerItem {
          override def onBind() {
            probe.ref ! "BOUND"
          }
          def receivedMessage(message: Any, sender: ActorRef){}
        }
        io ! IOCommand.BindWorkerItem(() => new MyItem)
        probe.expectMsg(100.milliseconds, "BOUND")
      }
    }



  }
}

