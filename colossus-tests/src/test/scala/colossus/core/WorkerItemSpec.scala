package colossus
package core

import testkit._

import akka.actor.ActorRef
import akka.testkit.TestProbe

import scala.concurrent.duration._

class WorkerItemSpec extends ColossusSpec {

  "A WorkerItem" must {
    "bind to a worker" in {
      withIOSystem{io => 
        val probe = TestProbe()
        class MyItem extends WorkerItem {
          override def onBind() {
            probe.ref ! "BOUND"
          }
          def receivedMessage(message: Any, sender: ActorRef){}
        }
        io ! IOCommand.BindWorkerItem(new MyItem)
        probe.expectMsg(100.milliseconds, "BOUND")
      }
    }

    "not bind more than once" in {
      withIOSystem{io =>
        val probe = TestProbe()
        class MyItem extends WorkerItem {
          override def onBind() {
            probe.ref ! "BOUND"
          }
          def receivedMessage(message: Any, sender: ActorRef){}
        }
        val item = new MyItem
        io ! IOCommand.BindWorkerItem(item)
        probe.expectMsg(100.milliseconds, "BOUND")
        io ! IOCommand.BindWorkerItem(item)
        probe.expectNoMsg(100.milliseconds)
      }       

    }

    "receive messages after binding" in {
      withIOSystem{io =>
        val probe = TestProbe()
        class MyItem extends WorkerItem {
          override def onBind() {
            boundWorker.get.worker ! WorkerCommand.Message(id.get, "PING")
          }
          def receivedMessage(message: Any, sender: ActorRef){
            message match {
              case "PING" => probe.ref ! "PONG"
            }
          }
        }
        val item = new MyItem
        io ! IOCommand.BindWorkerItem(item)
        probe.expectMsg(100.milliseconds, "PONG")
      }       
    }

    "not receive messages after unbinding" in {
      withIOSystem{io =>
        val probe = TestProbe()
        class MyItem extends WorkerItem {
          override def onBind() {
            boundWorker.get.worker ! WorkerCommand.UnbindWorkerItem(id.get)
            boundWorker.get.worker ! WorkerCommand.Message(id.get, "PING")
          }
          def receivedMessage(message: Any, sender: ActorRef){
            message match {
              case "PING" => probe.ref ! "PONG"
            }
          }
        }
        val item = new MyItem
        io ! IOCommand.BindWorkerItem(item)
        probe.expectNoMsg(100.milliseconds)
      }
    }



  }
}

