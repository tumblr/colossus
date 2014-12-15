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

  "Task" must {
    "bind to a worker" in {
      withIOSystem {sys =>
        val probe = TestProbe()
        val task = new Task {
          def start(id: Long, worker: WorkerRef) {
            probe.ref.!("BOUND")(proxy)
          }
          def receivedMessage(message: Any, sender: ActorRef){}
        }
        sys ! BindWorkerItem( task)
        probe.expectMsg(500.milliseconds, "BOUND")
      }
    }    

    "receive a message" in {
      withIOSystem{ sys =>
        val probe = TestProbe()
        val task = new Task{
          def start(id: Long, worker: WorkerRef) {
            worker.!(WorkerCommand.Message(id, "SEND"))(proxy)
          }
          def receivedMessage(message: Any, sender: ActorRef){
            message match {
              case "SEND" => probe.ref.!("RECEIVED")(proxy)
            }
          }
        }
        sys ! BindWorkerItem( task)
        probe.expectMsg(500.milliseconds, "RECEIVED")
      }
    }

    "receive through proxy" in {
      withIOSystem{ sys => 
        val task = new Task{
          def start(id: Long, worker: WorkerRef){}
          def receivedMessage(message: Any, sender: ActorRef) {
            message match {
              case "PING" => sender.!("PONG")(proxy)
            }
          }
        }
        sys ! BindWorkerItem( task)
        task.proxy ! "PING"
        expectMsg(100.milliseconds, "PONG")
      }
    }

    "unbind using message" in {
      withIOSystem{ sys =>
        val probe = TestProbe()
        val task = new Task{
          def start(id: Long, worker: WorkerRef){}
          def receivedMessage(message: Any, sender: ActorRef) {
            message match {
              case "PING" => sender.!("PONG")(proxy)
            }
          }
          override def onUnbind() {
            probe.ref.!("UNBOUND")(proxy)
          }
        }
        sys ! BindWorkerItem( task)
        task.proxy ! "PING"
        expectMsg(100.milliseconds, "PONG")
        task.proxy ! TaskProxy.Unbind
        task.proxy ! "PING"
        expectNoMsg(100.milliseconds)
        probe.expectMsg(100.milliseconds, "UNBOUND")
        
      }
    }

    "unbind by killing proxy actor" in {
      withIOSystem{ sys =>
        val probe = TestProbe()
        val task = new Task{
          def start(id: Long, worker: WorkerRef){
            probe.ref.!("BOUND")(proxy)
          }
          def receivedMessage(message: Any, sender: ActorRef) {
          }
          override def onUnbind() {
            probe.ref.!("UNBOUND")(proxy)
          }
        }
        sys ! BindWorkerItem( task)
        probe.expectMsg(100.milliseconds, "BOUND")
        task.proxy ! PoisonPill
        probe.expectMsg(100.milliseconds, "UNBOUND")
        
      }

    }

  }

  "Basic Task" must {
    "startup" in {
      withIOSystem{ sys =>
        val probe = TestProbe()
        sys run new BasicTask {
          onStart{
            probe.ref.!(id)(proxy)
          }
        }
        probe.expectMsg(300.milliseconds, Some(1L))
      }
    }
    "receive and become" in {
      withIOSystem{ sys => 
        val probe = TestProbe()
        val proxy = sys run new BasicTask {
          onStart {
            become {
              case "PING" => become {
                case "PING2" => probe.ref.!("PONG")(proxy)
              }
            }
          }
        }
        proxy ! "PING"
        probe.expectNoMsg(200.milliseconds)
        proxy ! "PING2"
        probe.expectMsg(200.milliseconds, "PONG")
      }
    }
  }

  "Task DSL" must {
    "create simple task" in {
      withIOSystem{implicit sys => 
        val probe = TestProbe()
        val task = Task{ctx =>
          import ctx._
          probe.ref.!("HELLO")(proxy)
          become {
            case "PING" => sender.!("PONG")(proxy)
          }
        }
        probe.expectMsg(200.milliseconds, "HELLO")
        task ! "PING"
        expectMsg(200.milliseconds, "PONG")
      }
    }
  }

}

