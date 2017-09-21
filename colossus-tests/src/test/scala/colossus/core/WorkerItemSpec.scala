package colossus.core

import colossus.testkit.ColossusSpec
import akka.actor.ActorRef
import akka.testkit.TestProbe
import colossus.IOCommand

import scala.concurrent.duration._

class NoopWorkerItem(c: Context) extends WorkerItem {
  val context = c
}

class WorkerItemSpec extends ColossusSpec {

  "A WorkerItem" must {
    "bind to a worker" in {
      withIOSystem { io =>
        val probe = TestProbe()
        class MyItem(c: Context) extends NoopWorkerItem(c) {
          override def onBind() {
            probe.ref ! "BOUND"
          }
        }
        io ! IOCommand.BindWorkerItem(context => new MyItem(context))
        probe.expectMsg(100.milliseconds, "BOUND")
      }
    }

    "not bind more than once" in {
      withIOSystem { io =>
        val probe = TestProbe()
        class MyItem(c: Context) extends NoopWorkerItem(c) {
          override def onBind() {
            probe.ref ! "BOUND"
            io ! IOCommand.BindWorkerItem(_ => this)
          }
        }
        io ! IOCommand.BindWorkerItem(context => new MyItem(context))
        probe.expectMsg(100.milliseconds, "BOUND")
        probe.expectNoMsg(100.milliseconds)
      }

    }

    "receive messages after binding" in {
      withIOSystem { io =>
        val probe = TestProbe()
        class MyItem(c: Context) extends NoopWorkerItem(c) {
          override def onBind() {
            worker.worker ! WorkerCommand.Message(id, "PING")
          }
          override def receivedMessage(message: Any, sender: ActorRef) {
            message match {
              case "PING" => probe.ref ! "PONG"
            }
          }
        }
        io ! IOCommand.BindWorkerItem(context => new MyItem(context))
        probe.expectMsg(100.milliseconds, "PONG")
      }
    }

    "not receive messages after unbinding" in {
      withIOSystem { io =>
        val probe = TestProbe()
        class MyItem(c: Context) extends NoopWorkerItem(c) {
          override def onBind() {
            worker.worker ! WorkerCommand.UnbindWorkerItem(id)
            worker.worker ! WorkerCommand.Message(id, "PING")
          }
          override def receivedMessage(message: Any, sender: ActorRef) {
            message match {
              case "PING" => probe.ref ! "PONG"
            }
          }
        }
        io ! IOCommand.BindWorkerItem(context => new MyItem(context))
        probe.expectNoMsg(100.milliseconds)
      }
    }

  }
}
