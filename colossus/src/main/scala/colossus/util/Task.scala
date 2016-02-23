package colossus
package task

import core._

import akka.actor._

case class TaskContext(proxy: ActorRef, workerContext: Context)

/**
 * A Task is basically a way to run an arbitrary function inside a worker.
 * Tasks can open connections and interact with actors through a built-in proxy
 * actor.
 *
 */

abstract class Task(context: TaskContext) extends WorkerItem(context.workerContext) {
  import TaskProxy._
  import Task._

  implicit val proxy = context.proxy

  override def onBind() {  
    proxy ! Bound(id, worker.worker)
    run()
  }

  def run()

  def receive: Receive = Map() //empty receive

  /**
   * Unbinds this Task from a Worker
   */
  def unbindTask() {
    proxy ! TaskProxy.Unbind
  }


  private var receiver: Receive = receive
  private var _sender: Option[ActorRef] = None
  def sender(): ActorRef = _sender.getOrElse(throw new Exception("No Sender!"))
  def become(it: Receive) {
    receiver = it
  }

  def receivedMessage(message: Any, sender: ActorRef) {
    _sender = Some(sender)
    receiver(message)
    _sender = None
  }

}

class TaskException(message: String) extends Exception(message)

object Task {

  type Receive = PartialFunction[Any, Unit]

  def start(creator: TaskContext => Task)(implicit io: IOSystem): ActorRef = {
    val proxy = io.actorSystem.actorOf(Props[TaskProxy])
    io ! IOCommand.BindWorkerItem(context => creator(TaskContext(proxy, context)))
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

