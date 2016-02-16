package colossus
package task

import core._

import akka.actor._

//TODO this whole thing needs a big overhaul

/**
 * A Task is basically a way to run an arbitrary function inside a worker.
 * Tasks can open connections and interact with actors through a built-in proxy
 * actor.
 *
 */

abstract class Task(val proxy: ActorRef, context: Context) extends WorkerItem(context) {
  import TaskProxy._
  import Task._

  override def onBind() {  
    proxy ! Bound(id, worker.worker)
    run()
  }

  def run()

  /**
   * Unbinds this Task from a Worker
   */
  def unbindTask() {
    proxy ! TaskProxy.Unbind
  }


  private var receiver: Receive = {
    case a => println(s"unhandled task message $a")
  }
  private var _sender: Option[ActorRef] = None
  def sender: ActorRef = _sender.getOrElse(throw new Exception("No Sender!"))
  def become(it: Receive) {
    receiver = it
  }
}

class TaskException(message: String) extends Exception(message)

object Task {

  type Receive = PartialFunction[Any, Unit]

  def apply(creator: ActorRef => Context => Task)(implicit io: IOSystem): ActorRef = {
    val proxy = io.actorSystem.actorOf(Props[TaskProxy])
    io ! IOCommand.BindWorkerItem(creator(proxy))
    proxy
  }
}

class TaskProxy extends Actor with ActorLogging with Stash{
  import TaskProxy._

  var unbindSent = false
  var _worker: Option[ActorRef] = None
  var _id = 0L

  def receive = {
    case Bound(id, worker) => { 
      _worker = Some(worker)
      _id = id
      context.become(bound(id, worker))
      unstashAll()
    }
    case other => stash()
  }

  def bound(id: Long, worker: ActorRef): Receive = {
    case Unbind => {
      unbindSent = true
      worker ! WorkerCommand.UnbindWorkerItem(id)

      context.stop(self)
    }
    case x => worker.!(WorkerCommand.Message(id, x))(sender())
  }

  override def postStop() {
    if (!unbindSent) {
      _worker.foreach{_ ! WorkerCommand.UnbindWorkerItem(_id)}
    }
  }
}
object TaskProxy {
  case class Bound(id: Long, worker: ActorRef)
  case object Unbind

}

