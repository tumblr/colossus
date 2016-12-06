package colossus
package task

import testkit._

import akka.actor._
import akka.testkit.TestProbe

import scala.concurrent.duration._

class TaskTest extends ColossusSpec {

  /*
   * Notice in these tests we have to explicitly provide the sender when
   * sending an actor message from within the task, since there's ambiguous
   * implicits (the task's self actor and the testkit implicit sender pulled
   * in from ColossusSpec).  Normally you do not need to do that
  */

  /*
   * NOTICE - in real life you don't need to explicitly pass the self as the
   * sender, just happens that the testkit already has an implicit sender
   */

  "Task" must {
    "bind to a worker" in {
      withIOSystem {implicit io =>
        val probe = TestProbe()
        Task.start(new Task(_) {
          def run() {
            probe.ref.!("BOUND")(self)
          }

          def receive = {case _ => ()}
        })

        probe.expectMsg(500.milliseconds, "BOUND")
      }
    }

    "receive a message through the proxy" in {
      withIOSystem{ implicit io =>
        val probe = TestProbe()
        val task = Task.start(new Task(_) {
          def run(){
          }


          def receive = {
            case "PING" => probe.ref.!( "PONG")(self)
          }
        })
        task ! "PING"
        probe.expectMsg(100.milliseconds, "PONG")
      }

    }

    "receive through self" in {
      withIOSystem{ implicit io =>
        val probe = TestProbe()
        val task = Task.start(new Task(_) {
          def run(){
            self ! "PING"
          }


          def receive = {
            case "PING" => probe.ref.!( "PONG")(self)
          }
        })
        probe.expectMsg(100.milliseconds, "PONG")
      }
    }

    "unbind by killing self actor" in {
      withIOSystem{ implicit io =>
        val probe = TestProbe()
        val task = Task.start(new Task(_) {
          def run(){
            probe.ref.!( "BOUND")(self)
          }
          def receive = {case _ => ()}

          override def onUnbind() {
            super.onUnbind()
            probe.ref.!( "UNBOUND")(self)
          }
        })
        probe.expectMsg(100.milliseconds, "BOUND")
        task ! PoisonPill
        probe.expectMsg(100.milliseconds, "UNBOUND")

      }

    }

  }


}

