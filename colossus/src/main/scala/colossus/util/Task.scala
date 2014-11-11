package colossus
package task

import core._

import akka.actor._

/**
 * A Task is basically a way to run an arbitrary function inside a worker.
 * Tasks can open connections and interact with actors through a built-in proxy
 * actor.
 *
 */

abstract class Task(implicit factory: ActorRefFactory) extends BindableWorkerItem {
  implicit val proxy = factory.actorOf(Props[TaskProxy])
  import TaskProxy._

  def bound(id :Long, worker: WorkerRef) {  
    proxy ! Bound(id, worker.worker)
    start(id, worker)
  }

  def start(id: Long, worker: WorkerRef)


  /**
   * Unbinds this Task from a Worker
   */
  def unbind() {
    proxy ! TaskProxy.Unbind
  }
}

class TaskException(message: String) extends Exception(message)

trait TaskContext {
  implicit val proxy: ActorRef
  def id: Long 
  def worker: WorkerRef

  def sender: ActorRef
  def become(it: Task.Receive)
}
  

class BasicTask(implicit factory: ActorRefFactory) extends Task with TaskContext {
  import Task._

  //private stuff

  private var startFunc: Function0[Unit] = () => ()
  private var _id: Option[Long] = None
  private var _worker: Option[WorkerRef] = None
  private var receiver: Receive = {
    case a => println(s"unhandled task message $a")
  }
  private var _sender: Option[ActorRef] = None


  def start(i: Long, w: WorkerRef) {
    _id = Some(i)
    _worker = Some(w)
    startFunc()
  }

  def receivedMessage(message: Any, sender: ActorRef){
    _sender = Some(sender)
    receiver(message)
  }

  //api methods

  def id: Long = _id.getOrElse(throw new TaskException("Cannot access id, task not bound"))
  def worker: WorkerRef = _worker.getOrElse(throw new TaskException("Cannot access worker, task not bound"))

  def onStart(f: => Unit) {
    startFunc = () => f
  }

  def sender: ActorRef = _sender.getOrElse(throw new Exception("No Sender!"))
  def become(it: Receive) {
    receiver = it
  }
  def run(f: => Any) {
    f
    unbind()
  }
}

object Task {

  type Receive = PartialFunction[Any, Unit]

  def apply(runner: TaskContext => Unit)(implicit io: IOSystem): ActorRef = {
    val task: BasicTask = new BasicTask()(io.actorSystem)
    task.onStart(runner(task))
    io ! IOCommand.BindWorkerItem(task)
    task.proxy
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

