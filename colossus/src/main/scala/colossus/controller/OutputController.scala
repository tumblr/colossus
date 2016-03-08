package colossus
package controller

import core._
import java.util.LinkedList
import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.util.{Success, Failure}

import service.{NotConnectedException, RequestTimeoutException}

/**
 * The DataQueue is used only when processing a stream.  It is used to buffer
 * data in between when the stream produces it and when it is ready to write to
 * the output buffer.  It also ensures that we don't pull too much data from a
 * stream, for example if a stream is infinite
 */
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


sealed trait OutputState {
  def canPush : Boolean
}
sealed trait AliveOutputState extends OutputState {
  val canPush = true
}

object OutputState{

  case object Dequeueing extends AliveOutputState
  case object PausedMessages extends AliveOutputState
  case class  Streaming(source: Source[DataBuffer], dataQ: DataQueue, post : QueuedItem.PostWrite) extends AliveOutputState
  //case class PausedStream(source: Source[DataBuffer], dataQ: DataQueue) extends OutputState
  case object Suspended extends OutputState {
    def canPush = true
  }
  case object Terminated extends OutputState {
    def canPush = false
  }

}

/**
 * This is thrown anytime we hit a state that shouldn't be possible.  If this
 * is ever thrown, there is a bug!
 */
class InvalidOutputStateException(message: String) extends Exception(message)


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

/**
 * The OutputController maintains all state dealing with writing messages in a
 * controller.  It maintains a queue of messages pending write and properly
 * handles writing both regular messages as well as streams.
 */
trait OutputController[Input, Output] extends MasterController[Input, Output] {
  import OutputState._
  import LiveState._

  private[controller] var outputState: OutputState[Output] = Suspended
  private var msgq = new MessageQueue[Output](controllerConfig.outputBufferSize)

  //TODO : This functionality is only used for (and only makes sense) the
  //ServiceClient.  It probably makes more sense to let ServiceClient do its own
  //buffering
  private var _writesEnabled = true

  private var manualDisconnect = false

  private[controller] def outputOnConnected() {

    outputState = WritingMessages
    _writesEnabled = true

    if (!msgq.isEmpty) {
      signalWrite()
    }

  }

  private[controller] def outputOnClosed() {
    val reason = new NotConnectedException("Connection Closed")
    outputState match {
      case Streaming(source, dataQ, post) => {
        source.terminate(reason)
        post(OutputResult.Failure(reason))
      }
      case _ => {}
    }
    if (manualDisconnect) {
      while (!msgq.isEmpty) {
        msgq.dequeue.postWrite(OutputResult.Cancelled(reason))
      }

      outputState = Terminated
    } else {
      outputState = Suspended
    }
  }

  /**
   * handle the output side of gracefully disconnecting.  If the output
   * controller is currently idle, we can immediately switch the state to
   * Terminated, otherwise, we switch disconnecting to true and wait for pending
   * messages to either get sent or timeout
   */
  private[controller] def outputGracefulDisconnect() {
    manualDisconnect = true 
    outputState match {
      case a : AliveOutputState => {
        checkOutputGracefulDisconnect()
      }
      case other => {
        val reason = new NotConnectedException("Connection Closed")
        while (!msgq.isEmpty) {
          msgq.dequeue.postWrite(OutputResult.Cancelled(reason))
        }
        outputState = Terminated
      }
    }
  }

  
  /**
   * returns true if the state is switched to Terminated
   */
  private def checkOutputGracefulDisconnect(): Boolean = {
    if (manualDisconnect && msgq.isEmpty && outputState == Dequeueing) {
      outputState = Terminated
      checkControllerGracefulDisconnect()
      true
    } else false
  }

  private def signalWrite() {
    connectionState match {
      case a: AliveState => {
        a.endpoint.requestWrite()
      }
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
    if (canPush) {
      if (msgq.size == 0 && outputState == Dequeueing) signalWrite()
      msgq.enqueue(item, postWrite, createdMillis)
      true
    } else {
      false
    }
  }

  protected def canPush = outputState.canPush && !msgq.isFull

  private def drainSource(){ 
    outputState match {
      case Streaming(source, dataQ, post) => source.pull {
        case Success(Some(data)) => {
          if (ls.dataQ.itemSize == 0) signalWrite()
          ls.dataQ.enqueue(data)
          if (! ls.dataQ.isFull) drainSource(ls)
        }
        case Success(None) => {
          //there's actually nothing to do here, switching back to the dequeuing
          //state is handled in readyForData
          if (!ls.dataQ.isEmpty) signalWrite()
        }
        case Failure(err) => {
          //todo: where to propagate the error?
          disconnect()
        }
      }
      case other => throw new InvalidOutputStateException("Attempted to drain source when not in streaming state")
    }
  }


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
          if (!s.msgQ.isEmpty) signalWrite()
        }
        case _ => {} //pausing doesn't affect streaming, so nothing to do
      }
    }
  }

  def readyForData(buffer: DataOutBuffer) = outputState match {
    case state: Alive[Output] => {
      if (checkOutputGracefulDisconnect(state)) {
        MoreDataResult.Complete
      } else state.liveState match {
        case Dequeueing => {
          var streamState: Option[Streaming] = None
          var continue = true
          while (writesEnabled && continue && state.msgQ.size > 0 && ! buffer.isOverflowed) {
            val next = state.msgQ.dequeue
            codec.encode(next.item) match {
              case e: Encoder => {
                e.encode(buffer)
                next.postWrite(OutputResult.Success)
              }
              case DataStream(source) => {
                val ls = Streaming(source, new DataQueue(1000), next.postWrite)
                streamState = Some(ls)
                outputState = state.copy(liveState = ls)
                drainSource(ls)
                continue = false
              }
            }
          }
          //TODO this can be cleaned up somehow
          if (continue) {
            if (state.disconnecting || state.msgQ.size > 0) {
              //return incomplete only if we overflowed the buffer and have more
              //in the queue, or id disconnecting to finish the disconnect
              //process
              MoreDataResult.Incomplete
            } else {
              MoreDataResult.Complete
            }
          } else {
            if (state.disconnecting || !streamState.get.dataQ.isEmpty) {
              MoreDataResult.Incomplete
            } else {
              MoreDataResult.Complete
            }
          }
        }
        case ls @ Streaming(source, dataQ, post) => {
          while (dataQ.itemSize > 0 && ! buffer.isOverflowed) {
            dataQ.dequeue.encode(buffer)
          }
          if (dataQ.isEmpty) {
            if (source.isClosed) {
              //all done
              post(OutputResult.Success)
              outputState = state.copy(liveState = Dequeueing)
            } else {
              //ask for more
              drainSource(ls)
            }
          }
          if (!dataQ.isEmpty || state.disconnecting) {
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

