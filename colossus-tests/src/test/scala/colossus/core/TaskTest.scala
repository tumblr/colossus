package colossus
package task

import testkit._

import core._

import akka.actor._
import akka.testkit.TestProbe

import scala.concurrent.duration._

import org.scalatest.Tag

class TaskTest extends ColossusSpec {
  import IOCommand.BindWorkerItem

  /* 
   * Notice in these tests we have to explicitly provide the sender when
   * sending an actor message from within the task, since there's ambiguous
   * implicits (the task's proxy actor and the testkit implicit sender pulled
   * in from ColossusSpec).  Normally you do not need to do that
  */

  /*
   * NOTICE - in real life you don't need to explicitly pass the proxy as the
   * sender, just happens that the testkit already has an implicit sender
   */

  "Task" must {
    "bind to a worker" in {
      withIOSystem {implicit io =>
        val probe = TestProbe()
        Task.start(new Task(_) {
          def run() {
            probe.ref.!("BOUND")(proxy)
          }
        })
            
        probe.expectMsg(500.milliseconds, "BOUND")
      }
    }    

    "receive a message through worker" in {
      withIOSystem{ implicit io =>
        val probe = TestProbe()
        Task.start(new Task(_) {
          def run(){
            worker.!(WorkerCommand.Message(id, "SEND"))(proxy)
          }

          override def receive = {
            case "SEND" => probe.ref.!("RECEIVED")(proxy)
          }
        })
        probe.expectMsg(500.milliseconds, "RECEIVED")
      }
    }

    "receive through proxy" in {
      withIOSystem{ implicit io => 
        val task = Task.start(new Task(_) {
          def run(){}

          override def receive = {
            case "PING" => sender.!( "PONG")(proxy)
          }
        })
        task ! "PING"
        expectMsg(100.milliseconds, "PONG")
      }
    }

    "unbind by killing proxy actor" in {
      withIOSystem{ implicit io =>
        val probe = TestProbe()
        val task = Task.start(new Task(_) {
          def run(){
            probe.ref.!( "BOUND")(proxy)
          }

          override def onUnbind() {
            super.onUnbind()
            probe.ref.!( "UNBOUND")(proxy)
          }
        })
        probe.expectMsg(100.milliseconds, "BOUND")
        task ! PoisonPill
        probe.expectMsg(100.milliseconds, "UNBOUND")
        
      }

    }

  }


}

