package colossus
package controller

import core._
import colossus.service.{DecodedResult, Codec}
import scala.util.{Success, Failure}
import PushResult._

/** trait representing a decoded message that is actually a stream
 * 
 * When a codec decodes a message that contains a stream (perhaps an http
 * request with a very large body), the returned message MUST extend this
 * trait, otherwise the InputController will not know to push subsequent data
 * to the stream message and things will get all messed up. 
 */
trait StreamMessage {
  def sink: Sink[DataBuffer]
}

case class ControllerConfig(
  outputBufferSize: Int
)

//used to terminate input streams when a connection is closing
class DisconnectingException(message: String) extends Exception(message)

//passed to the onWrite handler to indicate the status of the write for a
//message
sealed trait OutputResult
object OutputResult {

  // the message was successfully written
  case object Success extends OutputResult

  // the message failed, most likely due to the connection closing partway
  case object Failure extends OutputResult

  // the message was cancelled before it was written (not implemented yet) 
  case object Cancelled extends OutputResult
}

abstract class Controller[Input, Output](val codec: Codec[Output, Input], val controllerConfig: ControllerConfig) 
extends ConnectionHandler {

  private var endpoint: Option[WriteEndpoint] = None
  def isConnected = endpoint.isDefined

  //TODO: all of this would probably be cleaner with some kind of state machine

  //represents a message queued for writing
  case class QueuedItem(item: Output, postWrite: OutputResult => Unit)
  //the queue of items waiting to be written.
  private val waitingToSend = new java.util.LinkedList[QueuedItem]

  //only one of these will ever be filled at a time, todo: make either
  private var currentlyWriting: Option[QueuedItem] = None
  private var currentStream: Option[(QueuedItem, Source[DataBuffer])] = None

  //set when streaming input into a message
  private var currentSink: Option[Sink[DataBuffer]] = None
  //set when input stream fills during writing
  private var currentTrigger: Option[Trigger] = None


  //whether or not we should be pulling items out of the queue to write, this can be set
  //to fale through pauseWrites
  private var writesEnabled = true

  //we need to keep track of this outside the write-endpoint in case the endpoint changes
  private var readsEnabled = true

  //set to true when graceful disconnect has been initiated
  private var disconnecting = false

  def connected(endpt: WriteEndpoint) {
    if (endpoint.isDefined) {
      throw new Exception("Handler Connected twice!")
    }
    endpoint = Some(endpt)
  }

  private def onClosed() {
    currentStream.foreach{case (item, source) =>
      source.terminate(new Exception("Connection Closed"))
      currentStream = None
    }

    currentSink.foreach{sink =>
      sink.terminate(new Exception("Connection Closed"))
      currentSink = None
      currentTrigger = None
    }
    endpoint = None
  }

  protected def connectionClosed(cause : DisconnectCause) {
    onClosed()
  }

  protected def connectionLost(cause : DisconnectError) {
    onClosed()
  }

  /****** OUTPUT ******/

  /** Push a message to be written
   * @param item the message to push
   * @param postWrite called either when writing has completed or failed
   */
  protected def push(item: Output)(postWrite: OutputResult => Unit): Boolean = {
    if (disconnecting) {
      false
    } else if (waitingToSend.size < controllerConfig.outputBufferSize) {
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
    currentlyWriting.foreach{queued => queued.postWrite(OutputResult.Failure)}
    currentlyWriting = None
    //TODO, move NotConnectedException
    currentStream.foreach{case (queued, sink) => sink.terminate(new service.NotConnectedException("Connection closed"))}
    currentStream = None

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
    writesEnabled = false
  }

  /**
   * Resumes writing of messages if currently paused, otherwise has no affect
   */
  protected def resumeWrites() {
    writesEnabled = true
    checkQueue()
  }

  protected def pauseReads() {
    readsEnabled = false
    endpoint.foreach{_.disableReads()}
  }

  protected def resumeReads {
    readsEnabled = true
    endpoint.foreach{_.enableReads()}
  }

  //todo: rename
  protected def paused = !writesEnabled

  def disconnect() {
    //this has to be public to be used for clients
    endpoint.foreach{_.disconnect()}
    endpoint = None
  }

  /**
   * stops reading from the connection and accepting new writes, but waits for
   * pending/ongoing write operations to complete before disconnecting
   */
  def gracefulDisconnect() {
    endpoint.foreach{_.disableReads()}
    disconnecting = true
    //maybe wait instead for the message to finish? probably not
    currentSink.foreach{_.terminate(new DisconnectingException("Connection is closing"))}
    currentTrigger.foreach{_.cancel()}
    checkGracefulDisconnect()
  }

  private def checkGracefulDisconnect() {
    if (disconnecting && waitingToSend.size == 0 && currentlyWriting.isEmpty && currentStream.isEmpty) {
      endpoint.foreach{_.disconnect()}
    }
  }

  // == PRIVATE MEMBERS ==



  /*
   * iterate through the queue and write items.  Writing non-streaming items is
   * iterative whereas writing a stream enters drain, which will be recursive
   * if the stream has multiple databuffers to immediately write
   */
  private def checkQueue() {
    while (writesEnabled && currentlyWriting.isEmpty && currentStream.isEmpty && waitingToSend.size > 0 && endpoint.isDefined) {
      val queued = waitingToSend.remove()
      codec.encode(queued.item) match {
        case DataStream(sink) => {
          drain(sink)
          currentStream = Some((queued, sink))
        }
        case d: DataBuffer => endpoint.get.write(d) match {
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
    checkGracefulDisconnect()
  }
      

  /*
   * keeps reading from a source until it's empty or writing a databuffer is
   * incomplete.  Notice in the latter case we just wait for readyForData to be
   * called and resume there
   */
  private def drain(source: Source[DataBuffer]) {
    source.pull{
      case Success(Some(data)) => endpoint.get.write(data) match {
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

  /******* INPUT *******/

  def receivedData(data: DataBuffer) {

    def resetSink(buffer : DataBuffer) {
      currentSink = None
      //recurse since the databuffer may still contain data for the next request
      if(buffer.hasUnreadData) receivedData(buffer)
    }

    def processAndContinue(msg : Input, buffer : DataBuffer) {
      processMessage(msg)
      if(buffer.hasUnreadData) receivedData(buffer)
    }

    currentSink match {
      case Some(sink) => sink.push(data) match {
        case Success(Ok) => {}
        case Success(Done) => resetSink(data)
        case Success(Full(trigger)) => {
          endpoint.get.disableReads()
          trigger.fill{() =>
            //todo: maybe do something if endpoint disappears before trigger is called.  Also maybe need to cancel trigger?
            endpoint.foreach{_.enableReads()}
            currentTrigger = None
          }
          currentTrigger = Some(trigger)
        }
        //TODO: are the following 2 cases really exceptions..seems like they are part of the normal control flow from a controller's POV
        case Failure(a : PipeTerminatedException) => resetSink(data)
        case Failure(a : PipeClosedException) => resetSink(data)
        case Failure(t : Throwable) => {
          //todo: when a non expected failure occurs?
        }
      }
      case None => codec.decode(data) match {
        case Some(DecodedResult.Streamed(msg, sink)) => {
          currentSink = Some(sink)
          processAndContinue(msg, data)
        }
        case Some(DecodedResult.Static(msg)) => processAndContinue(msg, data)
        case None => {}
      }
    }
  }


  protected def processMessage(message: Input)
}


