package colossus
package controller

import core._
import scala.concurrent.duration.Duration
import scala.util.{Success, Failure}

import service.{NotConnectedException, RequestTimeoutException}
import encoding._


sealed trait OutputState 
object OutputState{

  /**
   * The controller is not in the middle of writing a message and is actively
   * dequeuing pending messages.  This is the normal state when it is writing
   * non-streamed messages and the entire message is written in one call to
   * write
   */
  case object Dequeueing extends OutputState

  /**
   * The controller is in the middle of writing a single message.  This state
   * only occurs when the message was too large to write in one call, and we're
   * waiting for the socket's underlying buffer to clear out before writing the
   * rest
   */
  case class Writing(encoder: Encoder, postWrite: OutputResult => Unit) extends OutputState
  
  /**
   * In this state, the controller is effectively disabled.  This generally
   * only occurs once the underlying connection has been disconnected.  Be
   * aware that it is possible for a controller to re-establish a connection,
   * so Terminated is not necessarily an end state.
   */
  case object Terminated extends OutputState //only used when disconnecting
}

/**
 * This is thrown anytime we hit a state that shouldn't be possible.  If this
 * is ever thrown, there is a bug!
 */
class InvalidOutputStateException(state: OutputState) extends Exception(s"Invalid Output State: $state")


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

  case class QueuedItem(item: Output, postWrite: OutputResult => Unit, creationTimeMillis: Long) {
    def isTimedOut(now: Long) = controllerConfig.sendTimeout.isFinite && now > (creationTimeMillis + controllerConfig.sendTimeout.toMillis)
  }

  private[controller] var outputState: OutputState = Dequeueing

  //whether or not we should be pulling items out of the queue to write, this
  //can be set to false through pauseWrites
  private var _writesEnabled = true
  def writesEnabled = _writesEnabled

  //represents a message queued for writing
  //the queue of items waiting to be written.
  // this is intentionally a j.u.LinkedList instead of s.Queue for performance reasons
  private val waitingToSend = new java.util.LinkedList[QueuedItem]

  def queueSize = waitingToSend.size()
  def outputQueueFull: Boolean = queueSize == controllerConfig.outputBufferSize

  private[controller] def outputOnConnected() {
    _writesEnabled = true
    outputState = Dequeueing
    signalWrite()
  }

  private[controller] def outputOnClosed() {
    val reason = new NotConnectedException("Connection Closed")
    outputState match {
      case Writing(_, post) => {
        post(OutputResult.Failure(reason))
      }
      case _ => {}
    }
    outputState = Terminated
  }

  private[controller] def outputGracefulDisconnect() {
    if (outputState == Dequeueing) {
      outputState = Terminated
    }
  }

  private def signalWrite() {
    if (waitingToSend.size > 0) {
      state match {
        case ConnectionState.Connected(endpoint) => endpoint.requestWrite()
        case _ => {}
      }
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
  protected def push(item: Output, createdMillis: Long = System.currentTimeMillis)(postWrite: OutputResult => Unit): Boolean = {
    if (waitingToSend.size < controllerConfig.outputBufferSize) {
      waitingToSend.add(QueuedItem(item, postWrite, createdMillis))
      signalWrite()
      true
    } else {
      false
    }
  }

  /** Purge the outgoing message, if there is one
   *
   * If a message is currently being streamed, the stream will be terminated
   */
  protected def purgeOutgoing(reason: Throwable) {
    outputState match {
      case Writing(_, postWrite) => postWrite(OutputResult.Failure(reason))
      case _ => {}
    }
    outputState = state match {
      case d: ConnectionState.Disconnecting => Terminated
      case _ => Dequeueing
    }
  }

  /** Purge all pending messages
   * 
   * If a message is currently being written, it is not affected
   */
  protected def purgePending(reason: Throwable) {
    while (waitingToSend.size > 0) {
      val q = waitingToSend.remove()
      q.postWrite(OutputResult.Cancelled(reason))
    }
  }

  /** Purge both pending and outgoing messages */
  protected def purgeAll(reason: Throwable) {
    purgeOutgoing(reason)
    purgePending(reason)
  }

  /**
   * Pauses writing of the next item in the queue.  If there is currently a
   * message in the process of writing, it will be unaffected.  New messages
   * can still be pushed to the queue as long as it is not full
   */
  protected def pauseWrites() {
    _writesEnabled = false
  }

  /**
   * Resumes writing of messages if currently paused, otherwise has no affect
   */
  protected def resumeWrites() {
    _writesEnabled = true
    signalWrite()
  }

  def readyForData(buffer: DataOutBuffer) = {
    //if we're in the middle of writing a message, try to complete it
    outputState match {
      case Writing(encoder, post) => {
        if (encoder.writeInto(buffer) == EncodeResult.Complete) {
          post(OutputResult.Success)
          outputState = Dequeueing
        }
      }
      case _ => {}
    }
    while (outputState == Dequeueing && waitingToSend.size > 0 && _writesEnabled) {
      val queued = waitingToSend.remove()
      val encoder = codec.encode(queued.item)
      encoder.writeInto(buffer) match {
        case EncodeResult.Complete    => queued.postWrite(OutputResult.Success)
        case EncodeResult.Incomplete  => outputState = Writing(encoder, queued.postWrite)
      }
    }
    checkControllerGracefulDisconnect()

    outputState match {
      case Dequeueing => MoreDataResult.Complete
      case Writing(_,_) => MoreDataResult.Incomplete
      case Terminated => throw new InvalidOutputStateException(outputState)
    }
  }

  def idleCheck(period: Duration): Unit = {
    val time = System.currentTimeMillis
    while (waitingToSend.size > 0 && waitingToSend.peek.isTimedOut(time)) {
      val expired = waitingToSend.removeFirst()
      expired.postWrite(OutputResult.Cancelled(new RequestTimeoutException))
    }
  }

}
