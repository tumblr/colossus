package colossus
package core

import akka.actor._
import akka.event.LoggingAdapter

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, SocketChannel}
import scala.concurrent.duration.FiniteDuration

class WorkerItemException(message: String) extends Exception(message)

/**
 * Represents the binding of an item to a worker
 *
 * @param id the id used to reference the worker item
 * @param worker the worker the item is bound to
 */
class Context(val id: Long, val worker: WorkerRef) {

  def !(message: Any)(implicit sender: ActorRef) {
    worker.worker ! WorkerCommand.Message(id, message)
  }

  def unbind() {
    worker.worker ! WorkerCommand.UnbindWorkerItem(id)
  }

  private[colossus] lazy val proxy : ActorRef = {
    _proxyExists = true
    worker.system.actorSystem.actorOf(Props(classOf[WorkerItemProxy], id, worker))
  }

  private var _proxyExists = false
  private[colossus] def proxyExists = _proxyExists
}


/**
 * A WorkerItem is anything that can be bound to worker to receive both events
 * and external messages.  WorkerItems are expected to be single-threaded and
 * non-blocking.  Once a WorkerItem is bound to a worker, all of its methods
 * are executed in the event-loop thread of the bound worker.
 *
 * Note - WorkerItems currently do not automatically bind to a worker on
 * construction.  This is because a worker does the binding itself for server
 * connections immediately on construction.  Clients need to bind themselves,
 * except when created through the BindAndCreateWorkerItem message.  Maybe this
 * should change.
 *
 * Note - WorkerItem cannot simply just always generate its own context, since
 * in some cases we want one WorkerItem to replace another, in which case the
 * context must be transferred
 */
abstract class WorkerItem(val context: Context) {

  def this(worker: WorkerRef) = this(worker.generateContext())

  def id = context.id
  def worker = context.worker

  private var bound = false

  def isBound = bound

  /**
   * Unbinds the WorkerItem, if it is bound.  When unbinding is complete,
   * `onUnbind()` is called.  This method is asynchronous.
   */
  private[colossus] def unbind() {
    if (!isBound) {
      throw new WorkerItemException(s"Cannot unbind WorkerItem, not bound to any worker")
    }
    worker.unbind(id)
  }


  /**
   * Signal from the worker to the item that it is now bound
   * @param id  The id assigned to this Item.
   * @param worker The Worker whom was bound
   */
  private[colossus] def setBind() {
    bound = true
    onBind()
  }

  /**
   * Called when this item is unbound from a Worker.
   */
  private[colossus] def setUnbind(){
    bound = false
    onUnbind()
  }

  /**
   * Provides a way to send this WorkerItem a message from an Actor by way of
   * WorkerCommand.Message.
   * @param message  The message that was sent
   * @param sender The sender who sent the message
   */
  def receivedMessage(message: Any, sender: ActorRef)

  /**
   * Called when the item is bound to a worker.
   */
  protected def onBind(){}

  /**
   * Called when the item has been unbound from a worker
   */
  protected def onUnbind(){}


  /**
   * A Request has been made to shutdown this WorkerItem.  By default this will
   * simply unbind the item from its Worker, but this can be overriden to add in
   * custom shutdown behaviors.  Be aware that in some cases this method may not
   * be called and the item will be unbound, such as when an IOSystem is
   * shutting down.
   */
  def shutdownRequest() {
    unbind()
  }


}

/**
 * A mixin trait for worker items that defines a method which is periodically
 * called by the worker.  This can be used to do periodic checks
 */
trait IdleCheck extends WorkerItem {

  /**
   * Called periodically on every attached connection handler, this can be used
   * for checking if an ongoing operation has timed out.
   *
   * Be aware that this is totally independant of a connection's idle timeout,
   * which is only based on the last time there was any I/O.
   *
   * @param period the frequency at which this method is called.  Currently this
   * is hardcoded to `WorkerManager.IdleCheckFrequency`, but may become
   * application dependent in the future.
   */
  def idleCheck(period: FiniteDuration)

}

/**
 * This is a mixin for [[WorkerItem]] that gives it actor-like capabilities.  A
 * "proxy" Akka actor is spun up, such that any message sent to the proxy will
 * be relayed to the WorkerItem.
 *
 * The proxy actors lifecycle will be linked to the lifecycle of this
 * workeritem, so if the actor is kill, the `shutdownRequest` method will be
 * invoked, and likewise if this item is unbound the proxy actor will be killed.
 */
trait ProxyActor extends WorkerItem {

  import ProxyActor._

  implicit val self : ActorRef = context.proxy

  private var currentReceiver : Receive = receive

  def becomeReceive(receive: Receive) {
    currentReceiver = receive
  }

  override def onBind() {
    super.onBind()
    self ! WorkerItemProxy.Bound
  }

  override def onUnbind() {
    super.onUnbind()
    //notice we send this message instead of a PoisonPill so the proxy knows we
    //sent it and doesn't send a shutdown message
    if (!killedByProxy) {
      //if we were killed by the proxy it means the actor is already dead
      self ! WorkerItemProxy.Unbound
    }
  }

  private var lastSender = ActorRef.noSender
  private var killedByProxy = false
  def sender() : ActorRef = lastSender

  def receivedMessage(message: Any, sender: ActorRef) {
    lastSender = sender
    message match {
      case WorkerItemProxy.Shutdown => {
        killedByProxy = true
        shutdownRequest()
      }
      case other => currentReceiver.applyOrElse(message, (_: Any) => ())
    }
  }

  def receive : Receive

}

object ProxyActor {

  type Receive = PartialFunction[Any, Unit]

}

class WorkerItemProxy(id: Long, worker: WorkerRef) extends Actor with Stash {
  import WorkerItemProxy._

  var killedByItem = false

  def startup: Receive = {
    case Bound => {
      unstashAll()
      context.become(active)
    }
    case other => stash()
  }

  def sendToItem(message: Any) {
    worker.worker ! WorkerCommand.Message(id, message)
  }

  def active: Receive = {
    case Worker.MessageDeliveryFailed(_,_) => {} //do anything here?
    case Unbound => {
      killedByItem = true
      self ! PoisonPill
    }
    case x => sendToItem(x)

  }

  def receive = startup

  override def postStop() {
    if (!killedByItem) {
      sendToItem(WorkerItemProxy.Shutdown)
    }
  }

}

object WorkerItemProxy {
  sealed trait ProxyCommand
  case object Bound extends ProxyCommand
  case object Unbound extends ProxyCommand

  sealed trait ProxyToWorkerItem
  case object Shutdown extends ProxyToWorkerItem
}

