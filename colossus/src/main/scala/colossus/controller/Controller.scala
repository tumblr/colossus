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


sealed trait ConnectionState
sealed trait AliveState extends ConnectionState {
  def endpoint: WriteEndpoint
}
object ConnectionState {
  case object NotConnected extends ConnectionState
  case class Connected(endpoint: WriteEndpoint) extends ConnectionState with AliveState
  case class Disconnecting(endpoint: WriteEndpoint) extends ConnectionState with AliveState
}
class InvalidConnectionStateException(state: OutputState) extends Exception(s"Invalid connection State: $state")

sealed trait InputState
object InputState {
  //notice there is no idle state, we always default to decoding since that's
  //what we do when we receive the first bytes of a new message
  
  case object Decoding extends InputState
  case class ReadingStream(sink: Sink[DataBuffer]) extends InputState
  case class BlockedStream(sink: Sink[DataBuffer], continueTrigger: Trigger) extends InputState

  case object Terminated extends InputState //used when disconnecting, indicates we are not planning on doing aything more
}
class InvalidInputStateException(state: OutputState) extends Exception(s"Invalid Input State: $state")


//not being used yet
//case class QueuedItem[T](item: T, postWrite: OutputResult => Unit)

sealed trait OutputState 
object OutputState{
  case object Idle extends OutputState
  //using postWrite instead of queued item to avoid type param
  case class Writing(postWrite: OutputResult => Unit) extends OutputState
  case class Streaming(source: Source[DataBuffer], postWrite: OutputResult => Unit) extends OutputState

  case object Terminated extends OutputState //only used when disconnecting
}
class InvalidOutputStateException(state: OutputState) extends Exception(s"Invalid Output State: $state")

trait MasterController[Input, Output] extends ConnectionHandler {
  protected def state: ConnectionState
  protected def codec: Codec[Output, Input]
  protected def config: ControllerConfig

  //needs to be called after various actions complete to check if it's ok to disconnect
  def checkTermination()
}

trait InputController[Input, Output] extends MasterController[Input, Output] {
  import InputState._

  private var inputState: InputState = Decoding

  //we need to keep track of this outside the write-endpoint in case the endpoint changes
  //maybe not
  private var readsEnabled = true

  def inputOnClosed() {
    inputState match {
      case ReadingStream(sink) => {
        sink.terminate(new Exception("Connection Closed"))
      }
      case BlockedStream(sink, trigger) => {
        sink.terminate(new Exception("Connection Closed"))
        trigger.cancel()
      }
      case _ => {}
    }
    inputState = Terminated
  }

  protected def pauseReads() {
    readsEnabled = false
    endpoint.foreach{_.disableReads()}
  }

  protected def resumeReads {
    readsEnabled = true
    endpoint.foreach{_.enableReads()}
  }


  def receivedData(data: DataBuffer) {

    def processAndContinue(msg : Input, buffer : DataBuffer) {
      processMessage(msg)
      if(buffer.hasUnreadData) receivedData(buffer)
    }

    inputState match {
      case Decoding => codec.decode(data) match {
        case Some(DecodedResult.Streamed(msg, sink)) => {
          inputState = ReadingStream(sink)
          processAndContinue(msg, data)
        }
        case Some(DecodedResult.Static(msg)) => processAndContinue(msg, data)
        case None => {} //nothing to do here, just waiting for MOAR DATA
      }
      case ReadingStream(sink) => sink.push(data) match {
        case Success(Ok) => {}
        case Success(Done) => state match{
          case ConnectionState.Disconnecting(_) {
            //gracefulDisconnect was called, so we allowed the connection to
            //finish reading in the stream, but now that it's done, disable
            //reads and drop any data still in the buffer
            endpoint.foreach{_.disableReads()}
          case ConnectionState.Connected(_) => {
            inputState = Decoding
            if(buffer.hasUnreadData) receivedData(buffer)
          }
          case other => throw new InvalidConnectionStateException(other)
        }
        case Success(Full(trigger)) => state match {
          case a: AliveState => {
            a.endpoint.disableReads()
            trigger.fill{() =>
              //todo: maybe do something if endpoint disappears before trigger is called.  Also maybe need to cancel trigger?
              a.endpoint.foreach{_.enableReads()}
              inputState = ReadingStream(sink)
            }
            inputState = BlockedStream(sink, trigger)
          }
          case other => throw new InvalidConnectionStateException(other)
        }
        case Failure(a : PipeTerminatedException) => {
          //disconnect
          inputState = Terminated
        }
        case Failure(a : PipeClosedException) => {
          //this only happens on infinite pipes
          //TODO:  this should not be an exception/failure
          inputState = Decoding
          if(buffer.hasUnreadData) receivedData(buffer)
        }
        case Failure(t : Throwable) => {
          //todo: when a non expected failure occurs?
          //this can be probably merged with pipe-terminated
          inputState = Terminated
        }
      }
      case other => throw new InvalidInputStateException(other)
    }
  }


  protected def processMessage(message: Input)

}

trait OutputController[Input, Output] extends MasterController[Input, Output] {

  private var outputState: OutputState = Idle

  //whether or not we should be pulling items out of the queue to write, this
  //can be set to false through pauseWrites
  private var writesEnabled = true

  //represents a message queued for writing
  //the queue of items waiting to be written.
  private val waitingToSend = new java.util.LinkedList[QueuedItem]

  def outputOnClosed() {
    //FIX
    currentStream.foreach{case (item, source) =>
      source.terminate(new Exception("Connection Closed"))
      currentStream = None
    }

  }

  /** Push a message to be written
   * @param item the message to push
   * @param postWrite called either when writing has completed or failed
   */
  protected def push(item: Output)(postWrite: OutputResult => Unit): Boolean = {
    state match {
      case ConnectionState.Connected(_) if (waitingToSend.size < controllerConfig.outputBufferSize) => {
        waitingToSend.add(QueuedItem(item, postWrite))
        checkQueue() 
        true
      }
      case _ => false
    }
  }

  /** Purge the outgoing message, if there is one
   *
   * If a message is currently being streamed, the stream will be terminated
   */
  protected def purgeOutgoing() {
    outputState match {
      case Writing(postWrite) => postWrite(OutputResult.Failure)
      case Streaming(source) => source.terminate(new service.NotConnectedException("Connection closed"))
      case _ => {}
    }
    outputState = state match {
      case d: Disconnecting => Terminated
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
    writesEnabled = false
  }

  /**
   * Resumes writing of messages if currently paused, otherwise has no affect
   */
  protected def resumeWrites() {
    writesEnabled = true
    checkQueue()
  }

  /*
   * iterate through the queue and write items.  Writing non-streaming items is
   * iterative whereas writing a stream enters drain, which will be recursive
   * if the stream has multiple databuffers to immediately write
   */
  private def checkQueue() {
    def go(endpoint: WriteEndpoint) {
      while (writesEnabled && outputState == Idle && waitingToSend.size > 0) {
        val queued = waitingToSend.remove()
        codec.encode(queued.item) match {
          case DataStream(sink) => {
            outputState = Streaming(sink, queued.postWrite)
            drain(sink)
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
              outputState = Writing(queued.postWrite)
            }
          }
        }
      }
    }
    state match {
      case ConnectionState.Connected(e) => go(e)
      case ConnectionState.Disconnecting(e) => go(e)
      case _ => {}
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
        outputState match {
          case Streaming(s, postWrite) => {
            postWrite(OutputResult.Success)
            outputState = Idle
            checkQueue()
          }
          case other => throw new InvalidOutputStateException(other)
        }
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
  import ConnectionState._

  protected var state: ConnectionState = NotConnected
  def isConnected = state != NotConnected

  def connected(endpt: WriteEndpoint) {
    state match {
      case NotConnected => state = Connected(endpt)
      case other => throw new Exception(s"Received connected event in $other state")
    }
  }

  private def onClosed() {
    state = NotConnected
    inputOnClosed()
    outputOnClosed()
  }

  protected def connectionClosed(cause : DisconnectCause) {
    onClosed()
  }

  protected def connectionLost(cause : DisconnectError) {
    onClosed()
  }

  /****** OUTPUT ******/



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
    if (currentSink.isEmpty) {
      endpoint.foreach{_.disableReads()}
    }
    disconnecting = true
    //maybe wait instead for the message to finish? probably not
    checkGracefulDisconnect()
  }

  private def checkGracefulDisconnect() {
    (state, inputState, outputState) match {
      case (Disconnecting(endpoint), InputState.Terminated, OutputState.Terminated) => {
        endpoint.disconnect()
        state = NotConnected
      }
      case _ => {} 
    }
  }

}


