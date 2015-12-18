package colossus
package controller

import core._
import java.util.LinkedList
import scala.concurrent.duration.Duration
import scala.util.{Success, Failure}

import service.{NotConnectedException, RequestTimeoutException}
import encoding._


class DataQueue(maxBytes: Long) {

  private var total = 0L
  private val queue = new LinkedList[(DataBuffer, Long)]

  def dataSize = total
  def itemSize = queue.size

  def isFull = total >= maxBytes

  def head = queue.peek._1

  /**
   * returns true if the queue can accept more data
   */
  def enqueue(data: DataBuffer): Boolean = {
    val size = data.remaining
    total += size
    queue.add(data -> size)
    isFull
  }

  def dequeue: DataBuffer = {
    val (data, size) = queue.remove
    total -= size
    data
  }

}

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

  def enqueue(item: T, postWrite: QueuedItem.PostWrite, created: Long): Boolean = if (!isFull) {
    queue.add(QueuedItem(item, postWrite, created))
    true
  } else false

  def head = queue.peek

  def dequeue = queue.remove

}

    





sealed trait OutputState[+T] {
  def ifLive(l: OutputState.Alive[T] => Unit) {}
}

object OutputState{



  /**
   * The normal state for the output controller.  This state is largely
   * independant of the underlying connection state.  Even if a connection has
   * been severed, the output controller may remain in the alive state,
   * especially if this is a client connection that is attempting to reconnect.
   *
   * @param msgQ The list of messages pending to be sent
   * @param dataQ The queue of DataBuffers pending to be sent
   * @param listState whether we are streaming or not
   * @param disconnecting if true we allow currently enqueued messages to drain but not accept new messages, when queues are drained disconnect
   * @param writesEnabled set by the user, if false new messages are always buffered to msgQ
   */
  case class Alive[T](msgQ: MessageQueue[T], dataQ: DataQueue, liveState: LiveState, disconnecting: Boolean, writesEnabled: Boolean) extends OutputState[T] {
    override def ifLive(l: Alive[T] => Unit){ l(this) }
  }



  /**
   * The suspended state occurs when the controller is currently not able to
   * send messages, but is expected to in the future.  This occurs primarily
   * when the connection is being established or when an unexpected disconnect
   * has occurred.  In these cases, the output controller is still able to
   * buffer messages.
   *
   * This is the initial state of the output controller
   */
  case class Suspended[T](msgQ: MessageQueue[T]) extends OutputState[T]

  
  /**
   * In this state, the controller is effectively disabled.  This generally
   * only occurs once the underlying connection has been manually disconnected,
   * and this controller no longer intends to be used.
   */
  case object Terminated extends OutputState[Nothing] //only used when disconnecting
}

sealed trait LiveState
object LiveState {
  /**
   * The controller is not in the middle of writing a message and is actively
   * dequeuing pending messages.  This is the normal state when it is writing
   * non-streamed messages and the entire message is written in one call to
   * write
   */
  case object Dequeueing extends LiveState

  /**
   * The controller is actively writing out a stream.  No other messages can be
   * encoded or written until this stream completes.  If the stream is
   * terminated before completing, the connection must be closed.
   */
  case class Streaming(source: Source[DataBuffer], postWrite: OutputResult => Unit) extends LiveState
}
  

/**
 * This is thrown anytime we hit a state that shouldn't be possible.  If this
 * is ever thrown, there is a bug!
 */
class InvalidOutputStateException[T](state: OutputState[T]) extends Exception(s"Invalid Output State: $state")


/** An ADT representing the result of a pushing a message to write
*/
sealed trait OutputResult
object OutputResult {

  // the message was successfully written
  case object Success extends OutputResult

  // the message failed while it was being written, most likely due to the connection closing partway
  case class Failure(reason: Throwable) extends OutputResult

  // the message was cancelled before it was written
  case class Cancelled(reason: Throwable) extends OutputResult
}

/**
 * The OutputController maintains all state dealing with writing messages in a
 * controller.  It maintains a queue of messages pending write and properly
 * handles writing both regular messages as well as streams.
 */
trait OutputController[Input, Output] extends MasterController[Input, Output] {
  import OutputState._
  import LiveState._



  private[controller] var outputState: OutputState[Output] = Suspended(new MessageQueue[Output](controllerConfig.outputBufferSize))

  private[controller] def outputOnConnected() {

    val msgs = outputState match {
      case Suspended(msgs) => msgs
      case _ => new MessageQueue[Output](controllerConfig.outputBufferSize)
    }
    outputState = Alive(msgs, new DataQueue(1024), Dequeueing, disconnecting = false, writesEnabled = true)
    if (!msgs.isEmpty) {
      signalWrite()
    }

  }

  private[controller] def outputOnClosed() {
    val reason = new NotConnectedException("Connection Closed")
    outputState match {
      case Alive(msgq, dataq, livestate, disconnecting, writesEnabled) => {
        livestate match {
          case Streaming(source, post) => {
            source.terminate(reason)
            post(OutputResult.Failure(reason))
          }
          case _ => {}
        }
        if (disconnecting) {
          //todo: flush msgq
          outputState = Terminated
        } else {
          outputState = Suspended(msgq)
        }
      }
      case _ => {
        //do nothing?
      }
    }
  }

  private[controller] def outputGracefulDisconnect() {
    outputState = outputState match {
      case a @ Alive(_, _, _, false, _) => a.copy(disconnecting = true)
      case other => other
    }
  }

  private def signalWrite() {
    state match {
      case ConnectionState.Connected(endpoint) => endpoint.requestWrite()
      case _ => {}
    }
  }

  /** Push a message to be written
   *
   * Pushing a message does not necessarily mean it will be written, but rather
   * that the message is queued to be written.  Messages can be queue
   * regardless of the state of the underlying connection, even if the
   * connection is never reconnected.  It is up to the caller to determine
   * whether a message should be pushed based on connection state.
   *
   * @param item the message to push
   * @param createdMillis the timestamp of when the message was created, defaults to now if not specified
   * @param postWrite called either when writing has completed or failed
   *
   * @return true if the message was successfully enqueued, false if the queue is full
   */
  protected def push(item: Output, createdMillis: Long = System.currentTimeMillis)(postWrite: QueuedItem.PostWrite): Boolean = {
    outputState match {
      case state @ Alive(msgq, _, _, false, _) if (!msgq.isFull) => {
        msgq.enqueue(item, postWrite, createdMillis)
        drainMessages(state)
        true
      }
      case _ => false
    }
  }

  private def drainMessages(state: Alive[Output]) {
    var cstate = state.liveState
    while (cstate == Dequeueing && !state.msgQ.isEmpty && !state.dataQ.isFull) {
      val next = state.msgQ.dequeue
      codec.encode(next.item) match {
        case d: DataBuffer => {
          //TODO: benchmark if the if-statement if necessary
          if (state.dataQ.itemSize == 0) signalWrite()
          state.dataQ.enqueue(d)
        }
        case DataStream(source) => {
          cstate = Streaming(source, next.postWrite)
          drainSource(source)
        }
      }

    }
    if (cstate != state.liveState) outputState = state.copy(liveState = cstate)
  }

  private def drainSource(source: Source[DataBuffer]){ source.pull{
    case Success(Some(data)) => outputState match {
      case a: Alive[Output] => {
        if (a.dataQ.itemSize == 0) signalWrite()
        a.dataQ.enqueue(data)
        if (!a.dataQ.isFull) {
          drainSource(source)
        }
      }
      case other => {
        source.terminate(new NotConnectedException("Connection Closed"))
      }
    }
    case Success(None) => {
      outputState.ifLive{s =>
        outputState = s.copy(liveState = Dequeueing)
        drainMessages(s)
      }
    }
    case Failure(err) => {
      //todo: where to propagate the error?
      disconnect()
    }
  }}


  /** Purge all pending messages
   * 
   * If a message is currently being written, it is not affected
   */
  protected def purgePending(reason: Throwable) {
    outputState match {
      case a : Alive[Output] => {
        while (!a.msgQ.isEmpty) {
          val q = a.msgQ.dequeue
          q.postWrite(OutputResult.Cancelled(reason))
        }
      }
    }
  }

  /** Purge both pending and outgoing messages */
  /**
   * Pauses writing of the next item in the queue.  If there is currently a
   * message in the process of writing, it will be unaffected.  New messages
   * can still be pushed to the queue as long as it is not full
   */
  protected def pauseWrites() {
    outputState.ifLive{s =>
      outputState = s.copy(writesEnabled = false)
    }
  }

  /**
   * Resumes writing of messages if currently paused, otherwise has no affect
   */
  protected def resumeWrites() {
    outputState.ifLive{s =>
      outputState = s.copy(writesEnabled = true)
      signalWrite()
    }
  }

  def readyForData(buffer: DataOutBuffer) = outputState match {
    case state: Alive[Output] => {
      val fullBefore = state.dataQ.isFull
      var continue = true
      while (continue) {
        val next = state.dataQ.head
        buffer.copy(next)
        if (next.remaining == 0) {
          state.dataQ.dequeue()
        } else {
          continue = false
        }
        if (state.dataQ.itemSize == 0) {
          continue = false
        }
      }
      //if dataQ can now accept more data, try to fill it again.
      if (fullBefore && !state.dataQ.isFull) {
        state.liveState match {
          case Dequeueing => drainMessages(state)
          case Streaming(source) => {
            drainSource(source)
          }
        }
      }
      if (dataQ.itemSize > 0) MoreDataResult.Incomplete else MoreDataResult.Complete
    }
    case _ => MoreDataResult.Complete //this should never happen
  }
        
  def idleCheck(period: Duration): Unit = {
    outputState.ifAlive{state =>
      val time = System.currentTimeMillis
      while (!state.msgQ.isEmpty && state.msgQ.head.isTimedOut(time, controllerConfig.sendTimeout)) {
        val expired = state.msgQ.dequeue
        expired.postWrite(OutputResult.Cancelled(new RequestTimeoutException))
      }
    }
  }

}
