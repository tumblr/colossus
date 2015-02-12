package colossus
package controller

import core._
import colossus.service.{DecodedResult, Codec}
import scala.util.{Success, Failure}

import service.NotConnectedException


sealed trait OutputState 
object OutputState{
  case object Idle extends OutputState
  //using postWrite instead of queued item to avoid type param
  case class Writing(postWrite: OutputResult => Unit) extends OutputState
  case class Streaming(source: Source[DataBuffer], postWrite: OutputResult => Unit) extends OutputState

  case object Terminated extends OutputState //only used when disconnecting
}
class InvalidOutputStateException(state: OutputState) extends Exception(s"Invalid Output State: $state")


/** An ADT representing the result of a pushing a message to write
*/
sealed trait OutputResult
object OutputResult {

  // the message was successfully written
  case object Success extends OutputResult

  // the message failed, most likely due to the connection closing partway
  case object Failure extends OutputResult

  // the message was cancelled before it was written (not implemented yet) 
  case object Cancelled extends OutputResult
}

/**
 * The OutputController maintains all state dealing with writing messages in a
 * controller.  It maintains a queue of messages pending write and properly
 * handles writing both regular messages as well as streams.
 */
trait OutputController[Input, Output] extends MasterController[Input, Output] {
  import OutputState._

  case class QueuedItem(item: Output, postWrite: OutputResult => Unit)

  private[controller] var outputState: OutputState = Idle

  //whether or not we should be pulling items out of the queue to write, this
  //can be set to false through pauseWrites
  private var _writesEnabled = true
  def writesEnabled = _writesEnabled

  //represents a message queued for writing
  //the queue of items waiting to be written.
  private val waitingToSend = new java.util.LinkedList[QueuedItem]

  private[controller] def outputOnConnected() {
    outputState = Idle
    checkQueue()
  }

  private[controller] def outputOnClosed() {
    outputState match {
      case Streaming(source, post) => {
        source.terminate(new NotConnectedException("Connection Closed"))
        post(OutputResult.Failure)
      }
      case _ => {}
    }
    outputState = Terminated
  }

  private[controller] def outputGracefulDisconnect() {
    if (outputState == Idle) {
      outputState = Terminated
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
   * @param postWrite called either when writing has completed or failed
   *
   * @return true if the message was successfully enqueued, false if the queue is full
   */
  protected def push(item: Output)(postWrite: OutputResult => Unit): Boolean = {
    if (waitingToSend.size < controllerConfig.outputBufferSize) {
      waitingToSend.add(QueuedItem(item, postWrite))
      checkQueue() 
      true
    } else {
      false
    }
  }

  /** Purge the outgoing message, if there is one
   *
   * If a message is currently being streamed, the stream will be terminated
   */
  protected def purgeOutgoing() {
    outputState match {
      case Writing(postWrite) => postWrite(OutputResult.Failure)
      case Streaming(source, post) => {
        source.terminate(new service.NotConnectedException("Connection closed"))
        post(OutputResult.Failure)
      }
      case _ => {}
    }
    outputState = state match {
      case d: ConnectionState.Disconnecting => Terminated
      case _ => Idle
    }
  }

  /** Purge all pending messages
   * 
   * If a message is currently being written, it is not affected
   */
  protected def purgePending() {
    while (waitingToSend.size > 0) {
      val q = waitingToSend.remove()
      q.postWrite(OutputResult.Cancelled)
    }
  }

  /** Purge both pending and outgoing messages */
  protected def purgeAll() {
    purgeOutgoing()
    purgePending()
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
    checkQueue()
  }

  /*
   * iterate through the queue and write items.  Writing non-streaming items is
   * iterative whereas writing a stream enters drain, which will be recursive
   * if the stream has multiple databuffers to immediately write
   */
  private def checkQueue() {
    def go(endpoint: WriteEndpoint) {
      while (_writesEnabled && outputState == Idle && waitingToSend.size > 0) {
        val queued = waitingToSend.remove()
        codec.encode(queued.item) match {
          case DataStream(sink) => {
            outputState = Streaming(sink, queued.postWrite)
            drain(sink)
          }
          case d: DataBuffer => endpoint.write(d) match {
            case WriteStatus.Complete => {
              queued.postWrite(OutputResult.Success)
            }
            case WriteStatus.Failed | WriteStatus.Zero => {
              //this probably shouldn't occur since we already check if the connection is writable
              queued.postWrite(OutputResult.Failure)
              //throw new Exception(s"Invalid write status")
            }
            case WriteStatus.Partial => {
              outputState = Writing(queued.postWrite)
            }
          }
        }
      }
    }
    state match {
      case a: AliveState if (waitingToSend.size > 0) => go(a.endpoint)
      case d: ConnectionState.Disconnecting => {
        outputState = Terminated
      }
      case _ => {}
    }
    checkControllerGracefulDisconnect()
  }
      

  /*
   * keeps reading from a source until it's empty or writing a databuffer is
   * incomplete.  Notice in the latter case we just wait for readyForData to be
   * called and resume there
   */
  private def drain(source: Source[DataBuffer]) {
    source.pull{
      case Success(Some(data)) => state match {
        case AliveState(endpoint) => endpoint.write(data) match {
          case WriteStatus.Complete => drain(source)
          case WriteStatus.Failed  => {
            source.terminate(new NotConnectedException("Connection closed during streaming"))
          }
          case WriteStatus.Zero => {
            throw new Exception("Invalid write status")
          }
          case WriteStatus.Partial =>{} //don't do anything, draining will resume in readyForData
        }
        case other => {
          source.terminate(new NotConnectedException("Connection closed during streaming"))
        }
      }
      case Success(None) => outputState match {
        case Streaming(s, postWrite) => {
          postWrite(OutputResult.Success)
          outputState = Idle
          checkQueue()
        }
        case other => throw new InvalidOutputStateException(other)
      }
      case Failure(err) => {
        //if we can't finish writing the current stream, not much else we can
        //do except close the connection
        throw err
      }
    }
  }

  /*
   * If we're currently streaming, resume the stream, otherwise when this is
   * called it means a non-stream item has finished fully writing, so we can go
   * back to checking the queue
   */
  def readyForData() {
    outputState match {
      case Streaming(sink, post) => drain(sink)
      case Writing(post) => {
        post(OutputResult.Success)
        outputState = Idle
        checkQueue()
      }
      case other => throw new InvalidOutputStateException(other)
    }
  }

}
