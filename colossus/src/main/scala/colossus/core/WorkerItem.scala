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
case class WorkerItemBinding(id: Long, worker: WorkerRef) {
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
 */
trait WorkerItem {
  private var _binding: Option[WorkerItemBinding] = None
  //todo - change these from Option[x] to just x and rethrow None exception as WorkerItemException
  def id = _binding.map{_.id}
  def boundWorker = _binding.map{_.worker}

  /** When bound to a worker, this contains the [WorkerItemBinding] */
  def binding = _binding
  def isBound = _binding.isDefined

  /**
   * Attempt to bind this WorkerItem to the worker.  When the binding succeeds,
   * `onBind()` is called and the item will be able to receive events and
   * messages.  Notice that this method is asynchronous.
   *
   * @param worker The worker to bind to
   */
  private[colossus] def bind(worker: WorkerRef) {
    if (isBound) {
      throw new WorkerItemException(s"Cannot bind WorkerItem, already bound with id ${id.get}")
    }
    boundWorker.get.bind(this)
  }

  /**
   * Unbinds the WorkerItem, if it is bound.  When unbinding is complete,
   * `onUnbind()` is called.  This method is asynchronous.
   */
  private[colossus] def unbind() {
    if (!isBound) {
      throw new WorkerItemException(s"Cannot unbind WorkerItem, not bound to any worker")
    }
    boundWorker.get.unbind(id.get)
  }


  /**
   * bind the item to a Worker.
   * @param id  The id assigned to this Item.
   * @param worker The Worker whom was bound
   */
  private[colossus] def setBind(id: Long, worker: WorkerRef) {
    _binding = Some(WorkerItemBinding(id, worker))
    onBind()
  }

  /**
   * Called when this item is unbound from a Worker.
   */
  private[colossus] def setUnbind(){
    _binding = None
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
