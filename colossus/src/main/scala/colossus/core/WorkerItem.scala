package colossus
package core

import akka.actor._
import akka.event.LoggingAdapter

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, SocketChannel}

class WorkerItemException(message: String) extends Exception(message)

/**
 * Represents the binding of an item to a worker
 *
 * @param id the id used to reference the worker item
 * @param worker the worker the item is bound to
 */
case class Context(id: Long, worker: WorkerRef) {
  def send(message: Any) {
    worker.worker ! WorkerCommand.Message(id, message)
  }
  def unbind() {
    worker.worker ! WorkerCommand.UnbindWorkerItem(id)
  }
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


}
