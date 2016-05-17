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
        class MyItem(c: Context) extends WorkerItem(c) {
          override def onBind() {
            probe.ref ! "BOUND"
          }
          def receivedMessage(message: Any, sender: ActorRef){}
        }
        io ! IOCommand.BindWorkerItem(new MyItem(_))
        probe.expectMsg(100.milliseconds, "BOUND")
      }
    }

    "not bind more than once" in {
      withIOSystem{io =>
        val probe = TestProbe()
        class MyItem(c: Context) extends WorkerItem(c) {
          override def onBind() {
            probe.ref ! "BOUND"
            io ! IOCommand.BindWorkerItem(_ => this)
          }
          def receivedMessage(message: Any, sender: ActorRef){}
        }
        io ! IOCommand.BindWorkerItem(new MyItem(_))
        probe.expectMsg(100.milliseconds, "BOUND")
        probe.expectNoMsg(100.milliseconds)
      }

    }

    "receive messages after binding" in {
      withIOSystem{io =>
        val probe = TestProbe()
        class MyItem(c: Context) extends WorkerItem(c) {
          override def onBind() {
            worker.worker ! WorkerCommand.Message(id, "PING")
          }
          def receivedMessage(message: Any, sender: ActorRef){
            message match {
              case "PING" => probe.ref ! "PONG"
            }
          }
        }
        io ! IOCommand.BindWorkerItem(new MyItem(_))
        probe.expectMsg(100.milliseconds, "PONG")
      }
    }

    "not receive messages after unbinding" in {
      withIOSystem{io =>
        val probe = TestProbe()
        class MyItem(c: Context) extends WorkerItem(c) {
          override def onBind() {
            worker.worker ! WorkerCommand.UnbindWorkerItem(id)
            worker.worker ! WorkerCommand.Message(id, "PING")
          }
          def receivedMessage(message: Any, sender: ActorRef){
            message match {
              case "PING" => probe.ref ! "PONG"
            }
          }
        }
        io ! IOCommand.BindWorkerItem(new MyItem(_))
        probe.expectNoMsg(100.milliseconds)
      }
    }



  }
}

