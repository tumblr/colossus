package colossus
package controller

import core._
import java.util.LinkedList
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.{Success, Failure}
import streaming._

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


  def writesEnabled = _writesEnabled

  override def onConnected() {
    super.onConnected()
    state = StaticOutState.Alive
    messages.peek match {
      case PullResult.Item(_) => signalWrite()
      case PullResult.Empty(signal) => signal.notify { signalWrite() }
      case other => ???
    }
  }


  private def onClosed() {
    if (disconnecting) {
      val reason = new NotConnectedException("Connection Closed")
      //purgePending(reason)
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
  /*
    val time = System.currentTimeMillis
    while (!outputBuffer.isEmpty && outputBuffer.head.isTimedOut(time, controllerConfig.sendTimeout)) {
      val expired = outputBuffer.dequeue
      expired.postWrite(OutputResult.Cancelled(new RequestTimeoutException))
    }
    */
  }

  private def signalWrite() {
    if (writesEnabled) {
      upstream.requestWrite()
    }
  }

  override def shutdown() {
    state = StaticOutState.Disconnecting
    checkShutdown()
  }

  private def checkShutdown() {
    if (disconnecting && !messages.canPullNonEmpty) {
      //we do this instead of shutting down immediately since this could be
      //called while in the middle of writing and end up prematurely closing the
      //connection
      signalWrite()
    }
  }


  def readyForData(buffer: DataOutBuffer) =  {
    if (disconnecting && messages.peek == PullResult.Item(())) {
      upstream.shutdown()
      MoreDataResult.Complete
    } else {
      val hasMore = messages.pullUntilNull { item =>
        codec.encode(item, buffer)
        !buffer.isOverflowed
      } match {
        case Some(PullResult.Empty(sig)) => {
          sig.notify{ signalWrite() }
          false
        }
        case Some(PullResult.Error(uhoh)) => ???
        case Some(PullResult.Closed) => ???
        case None => true //this would only occur if we returned false due to buffer overflowing
      }
      if (disconnecting || hasMore) {
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
