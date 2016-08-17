package colossus
package controller

import core._
import java.util.LinkedList
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.{Success, Failure}

import service.{NotConnectedException, RequestTimeoutException}


case class QueuedItem[T](item: T, postWrite: QueuedItem.PostWrite, creationTimeMillis: Long) {
  def isTimedOut(now: Long, timeout: Duration) = timeout.isFinite && now > (creationTimeMillis + timeout.toMillis)
}
object QueuedItem {
  type PostWrite = OutputResult => Unit
}

class MessageQueue[T](maxSize: Int) {

  private val queue = new LinkedList[QueuedItem[T]]

  def isEmpty = queue.size == 0
  def isFull = queue.size >= maxSize
  def size = queue.size

  def enqueue(item: T, postWrite: QueuedItem.PostWrite, created: Long): Boolean = if (!isFull) {
    queue.add(QueuedItem(item, postWrite, created))
    true
  } else false

  def head = queue.peek

  def dequeue = queue.remove

}

/** An ADT representing the result of a pushing a message to write
*/
sealed trait OutputResult
sealed trait OutputError extends OutputResult {
  def reason: Throwable
}
object OutputResult {

  // the message was successfully written
  case object Success extends OutputResult

  // the message failed while it was being written, most likely due to the connection closing partway
  case class Failure(reason: Throwable) extends OutputResult with OutputError

  // the message was cancelled before it was written
  case class Cancelled(reason: Throwable) extends OutputResult with OutputError
}

abstract class StaticOutState(val canPush: Boolean) {
  def disconnecting = !canPush
}
object StaticOutState {
  case object Suspended extends StaticOutState(true)
  case object Alive extends StaticOutState(true)
  case object Disconnecting extends StaticOutState(false)
  case object Terminated extends StaticOutState(false)
}

trait StaticOutputController[E <: Encoding] extends BaseController[E]{


  private var state: StaticOutState = StaticOutState.Suspended
  private def disconnecting = state.disconnecting
  private var _writesEnabled = true
  private var outputBuffer = new MessageQueue[E#Output](controllerConfig.outputBufferSize)

  def pendingBufferSize = outputBuffer.size

  def writesEnabled = _writesEnabled

  def pauseWrites() {
    _writesEnabled = false
  }

  /**
   * Resumes writing of messages if currently paused, otherwise has no affect
   */
  def resumeWrites() {
    _writesEnabled = true
    if (!outputBuffer.isEmpty) signalWrite()
  }

  def purgePending(reason: Throwable) {
    while (!outputBuffer.isEmpty) {
      outputBuffer.dequeue.postWrite(OutputResult.Cancelled(reason))
    }

  }

  override def onConnected() {
    super.onConnected()
    state = StaticOutState.Alive
    if (!outputBuffer.isEmpty) signalWrite()
  }


  private def onClosed() {
    if (disconnecting) {
      val reason = new NotConnectedException("Connection Closed")
      purgePending(reason)
    } 
  }

  protected def connectionClosed(cause : DisconnectCause) {
    state = StaticOutState.Terminated
    onClosed()
  }

  protected def connectionLost(cause : DisconnectError) {
    state = if (disconnecting) StaticOutState.Terminated else StaticOutState.Suspended
    onClosed()
  }

  override def onIdleCheck(period: FiniteDuration) {
    val time = System.currentTimeMillis
    while (!outputBuffer.isEmpty && outputBuffer.head.isTimedOut(time, controllerConfig.sendTimeout)) {
      val expired = outputBuffer.dequeue
      expired.postWrite(OutputResult.Cancelled(new RequestTimeoutException))
    }
  }

  //TODO: controller should not get access to this!!!
  private def signalWrite() {
    upstream.connectionState match {
      case a: AliveState => {
        if (writesEnabled) a.endpoint.requestWrite()
      }
      case _ => {}
    }
  }

  override def shutdown() {
    state = StaticOutState.Disconnecting
    checkShutdown()
  }

  private def checkShutdown() {
    if (disconnecting && outputBuffer.isEmpty) {
      upstream.shutdown()
    }
  }


  def canPush = state.canPush && !outputBuffer.isFull

  def push(item: E#Output, createdMillis: Long = System.currentTimeMillis)(postWrite: QueuedItem.PostWrite): Boolean = {
    if (canPush) {
      if (outputBuffer.isEmpty) signalWrite()
      outputBuffer.enqueue(item, postWrite, createdMillis)
      true
    } else false
  }

  def readyForData(buffer: DataOutBuffer) =  {
    if (disconnecting && outputBuffer.isEmpty) {
      checkShutdown()
      MoreDataResult.Complete
    } else {
      while (writesEnabled && outputBuffer.size > 0 && ! buffer.isOverflowed) {
        val next = outputBuffer.dequeue
        codec.encode(next.item, buffer)
        next.postWrite(OutputResult.Success)
      }
      if (disconnecting || (writesEnabled && outputBuffer.size > 0)) {
        //return incomplete only if we overflowed the buffer and have more in
        //the queue, or if there's nothing left but we're disconnecting to
        //finish the disconnect process
        MoreDataResult.Incomplete
      } else {
        MoreDataResult.Complete
      }
    }
  }
        

  

}
