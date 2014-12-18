package colossus
package controller

import scala.util.{Try, Success, Failure}
import core._


//passed to the onWrite handler to indicate the status of the write for a
//message
sealed trait OutputResult
object OutputResult {

  // the message was successfully written
  case object Success   extends OutputResult

  // the message failed, most likely due to the connection closing partway
  case object Failure   extends OutputResult

  // the message was cancelled before it was written (not implemented yet) 
  case object Cancelled extends OutputResult
}

/**
 * A mixin for ConnectionHandler that takes control of all write operations.
 * This controller is designed to accept messages to write, where a message may
 * be a (possibly infinite) stream
 *
 */
trait OutputController[Input, Output] extends ConnectionHandler with MessageHandler[Input, Output]{

  // == ABSTRACT MEMBERS ==
  
  // the write endpoint to control
  protected def writer: Option[WriteEndpoint]

  // == PUBLIC / PROTECTED MEMBERS ==

  /** Push a message to be written
   * @param item the message to push
   * @param postWrite called either when writing has completed or failed
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

  //messages being sent are failed, the rest are cancelled
  protected def purgeOutgoing() {
    currentlyWriting.foreach{queued => queued.postWrite(OutputResult.Failure)}
    currentlyWriting = None
    //TODO, move NotConnectedException
    currentStream.foreach{case (queued, sink) => sink.terminate(new service.NotConnectedException("Connection closed"))}
    currentStream = None

  }

  protected def purgePending() {
    while (waitingToSend.size > 0) {
      val q = waitingToSend.remove()
      q.postWrite(OutputResult.Cancelled)
    }
  }

  protected def purgeAll() {
    purgeOutgoing()
    purgePending()
  }

  /**
   * Pauses writing of the next item in the queue.  If there is currently a
   * message in the process of writing, it will be unaffected
   */
  protected def pauseWrites() {
    enabled = false
  }

  /**
   * Resumes writing of messages if currently paused, otherwise has no affect
   */
  protected def resumeWrites() {
    enabled = true
    checkQueue()
  }

  protected def paused = !enabled

  // == PRIVATE MEMBERS ==

  case class QueuedItem(item: Output, postWrite: OutputResult => Unit)

  //the queue of items waiting to be written.
  private val waitingToSend = new java.util.LinkedList[QueuedItem]

  //only one of these will ever be filled at a time
  private var currentlyWriting: Option[QueuedItem] = None
  private var currentStream: Option[(QueuedItem, Source[DataBuffer])] = None


  //whether or not we should be pulling items out of the queue, this can be set
  //to fale through pauseWrites
  private var enabled = true


  /*
   * iterate through the queue and write items.  Writing non-streaming items is
   * iterative whereas writing a stream enters drain, which will be recursive
   * if the stream has multiple databuffers to immediately write
   */
  private def checkQueue() {
    while (enabled && currentlyWriting.isEmpty && currentStream.isEmpty && waitingToSend.size > 0 && writer.isDefined) {
      val queued = waitingToSend.remove()
      codec.encode(queued.item) match {
        case DataStream(sink) => {
          drain(sink)
          currentStream = Some((queued, sink))
        }
        case d: DataBuffer => writer.get.write(d) match {
          case WriteStatus.Complete => {
            queued.postWrite(OutputResult.Success)
          }
          case WriteStatus.Failed | WriteStatus.Zero => {
            //this probably shouldn't occur since we already check if the connection is writable
            queued.postWrite(OutputResult.Failure)
          }
          case WriteStatus.Partial => {
            currentlyWriting = Some(queued)
          }
        }
      }
    }
  }
      

  /*
   * keeps reading from a sink until it's empty or writing a databuffer is
   * incomplete.  Notice in the latter case we just wait for readyForData to be
   * called and resume there
   */
  private def drain(source: Source[DataBuffer]) {
    source.pull{
      case Success(Some(data)) => writer.get.write(data) match {
        case WriteStatus.Complete => drain(source)
        case _ => {} //todo: maybe do something on Failure?
      }
      case Success(None) => {
        currentStream.foreach{case (q, s) => 
          q.postWrite(OutputResult.Success)
        }
        currentStream = None
        checkQueue()
      }
      case Failure(err) => {
        //TODO: what to do here?

      }
    }
  }

  /*
   * If we're currently streaming, resume the stream, otherwise when this is
   * called it means a non-stream item has finished fully writing, so we can go
   * back to checking the queue
   */
  def readyForData() {
    currentStream.map{case (q,s) => drain(s)}.getOrElse{
      currentlyWriting.foreach{q => q.postWrite(OutputResult.Success)}
      currentlyWriting = None
      checkQueue()
    }
  }

}
