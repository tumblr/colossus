package colossus
package controller

import core._
import java.util.LinkedList
import scala.annotation.tailrec
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
  def isEmpty = itemSize == 0

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
  def size = queue.size

  def enqueue(item: T, postWrite: QueuedItem.PostWrite, created: Long): Boolean = if (!isFull) {
    queue.add(QueuedItem(item, postWrite, created))
    true
  } else false

  def head = queue.peek

  def dequeue = queue.remove

}


sealed trait OutputState[T] {
  def ifAlive(l: OutputState.Alive[T] => Unit) {}
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
    override def ifAlive(l: Alive[T] => Unit){ l(this) }
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
  case class Terminated[T]() extends OutputState[T] //only used when disconnecting
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
  import QueuedItem._



  private[controller] var outputState: OutputState[Output] = Suspended(new MessageQueue[Output](controllerConfig.outputBufferSize))

  private[controller] def outputOnConnected() {

    val msgs = outputState match {
      case Suspended(msgs) => msgs
      case _ => new MessageQueue[Output](controllerConfig.outputBufferSize)
    }
    outputState = Alive(msgs, new DataQueue(controllerConfig.dataBufferSize), Dequeueing, disconnecting = false, writesEnabled = true)
    if (!msgs.isEmpty) {
      drainMessages()
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
          while (!msgq.isEmpty) {
            msgq.dequeue.postWrite(OutputResult.Cancelled(reason))
          }

          outputState = Terminated()
        } else {
          outputState = Suspended(msgq)
        }
      }
      case _ => {
        //do nothing?
      }
    }
  }

  /**
   * handle the output side of gracefully disconnecting.  If the output
   * controller is currently idle, we can immediately switch the state to
   * Terminated, otherwise, we switch disconnecting to true and wait for pending
   * messages to either get sent or timeout
   */
  private[controller] def outputGracefulDisconnect() {
    outputState match {
      case a @ Alive(_, _, _, false, _) => {
        val n = a.copy(disconnecting = true)
        if (!checkOutputGracefulDisconnect(n)) {
          outputState = n
        }
      }
      case Suspended(msgq) => {
        val reason = new NotConnectedException("Connection Closed")
        while (!msgq.isEmpty) {
          msgq.dequeue.postWrite(OutputResult.Cancelled(reason))
        }
        outputState = Terminated()
      }
      case other => {}
    }
  }

  
  /**
   * returns true if the state is switched to Terminated
   */
  private def checkOutputGracefulDisconnect(state: Alive[Output]): Boolean = {
    if (state.disconnecting && state.msgQ.isEmpty && state.dataQ.isEmpty && state.liveState == Dequeueing) {
      //this occurs as a continuation of below
      outputState = Terminated()
      checkControllerGracefulDisconnect()
      true
    } else false
  }

  private def signalWrite() {
    connectionState match {
      case a: AliveState => a.endpoint.requestWrite()
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
        drainMessages()
        true
      }
      case Suspended(msgq) if (!msgq.isFull) => {
        msgq.enqueue(item, postWrite, createdMillis)
        true
      }
      case _ => false
    }
  }

  protected def canPush = outputState match {
    case state @ Alive(msgq, _, _, false, _) if (!msgq.isFull) => {
      true
    }
    case Suspended(msgq) if (!msgq.isFull) => {
      true
    }
    case _ => false
  }

  @tailrec private def drainMessages() {
    outputState match {
      case a @ Alive(msgQ, dataQ, Dequeueing, _, true) if (!msgQ.isEmpty && !dataQ.isFull) => {
        val next = msgQ.dequeue
        codec.encode(next.item) match {
          case d: DataBuffer => {
            //TODO: benchmark if the if-statement if necessary
            if (dataQ.itemSize == 0) signalWrite()
            dataQ.enqueue(d)
            next.postWrite(OutputResult.Success)
            drainMessages()
          }
          case DataStream(source) => {
            val cstate = Streaming(source, next.postWrite)
            outputState = a.copy(liveState = cstate)
            drainSource(source, next.postWrite)
          }
        }
      }
      case _ => {}
    }
  }

  private def drainSource(source: Source[DataBuffer], post: PostWrite){ source.pull{
    case Success(Some(data)) => outputState match {
      case a: Alive[Output] => {
        if (a.dataQ.itemSize == 0) signalWrite()
        a.dataQ.enqueue(data)
        if (!a.dataQ.isFull) {
          drainSource(source, post)
        }
      }
      case other => {
        val ex = new NotConnectedException("Connection Closed")
        source.terminate(ex)
        post(OutputResult.Failure(ex))
      }
    }
    case Success(None) => {
      outputState.ifAlive{s =>
        val newState = s.copy(liveState = Dequeueing)
        outputState = newState
        post(OutputResult.Success)
        drainMessages()
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
      case Suspended(msgs) => {
        while (!msgs.isEmpty) {
          val q = msgs.dequeue
          q.postWrite(OutputResult.Cancelled(reason))
        }
      }
      case _ => {}
    }
  }

  protected def writesEnabled: Boolean = outputState match {
    case a: Alive[Output] if (a.writesEnabled) => true
    case _ => false
  }

  /** Purge both pending and outgoing messages */
  /**
   * Pauses writing of the next item in the queue.  If there is currently a
   * message in the process of writing, it will be unaffected.  New messages
   * can still be pushed to the queue as long as it is not full
   */
  protected def pauseWrites() {
    outputState.ifAlive{s =>
      outputState = s.copy(writesEnabled = false)
    }
  }

  /**
   * Resumes writing of messages if currently paused, otherwise has no affect
   */
  protected def resumeWrites() {
    outputState.ifAlive{s =>
      outputState = s.copy(writesEnabled = true)
      s.liveState match {
        case Dequeueing => {
          drainMessages()
        }
        case _ => {} //pausing doesn't affect streaming, so nothing to do
      }
    }
  }

  def readyForData(buffer: DataOutBuffer) = outputState match {
    case state: Alive[Output] => {
      if (checkOutputGracefulDisconnect(state)) {
        MoreDataResult.Complete
      } else {
        val fullBefore = state.dataQ.isFull
        var continue = state.dataQ.itemSize > 0
        while (continue) {
          val next = state.dataQ.head
          buffer.copy(next)
          if (next.remaining == 0) {
            state.dataQ.dequeue
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
            case Dequeueing => drainMessages()
            case Streaming(source, post) => {
              drainSource(source, post)
            }
          }
        }
        if (!state.dataQ.isEmpty || state.disconnecting) {
          //we return Incomplete when disconnecting because even though we have
          //nothing more to write, if we disconnect now then the data we just
          //buffered in this function call is not going to get written (since
          //that happens after this function returns), so we have to wait until
          //the next iteration to actually finish disconnecting.
          MoreDataResult.Incomplete
        } else {
          MoreDataResult.Complete
        }
      }
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

object OutputController {
  /**
   * The default max size for the data buffer of the output controller.
   *
   * TODO: This should be somehow synced up with the worker output buffer size,
   * or at the very least made configurable
   */
  val DefaultDataBufferSize = 16384
}
