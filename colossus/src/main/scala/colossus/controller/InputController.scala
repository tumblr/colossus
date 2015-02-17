package colossus
package controller

import core._
import colossus.service.{DecodedResult}
import scala.util.{Success, Failure}
import PushResult._

sealed trait InputState
object InputState {
  
  /**
   * The controller is waiting for more data to begin or continue decoding a message
   */
  case object Decoding extends InputState

  /**
   * The controller decoded a stream message and is routing incoming data into the stream
   */
  case class ReadingStream(sink: Sink[DataBuffer]) extends InputState

  /**
   * The controller is routing data into a stream, but the stream has indicated
   * that it's full, so we're waiting for the continueTrigger to be called,
   * which will resume reading in data
   */
  case class BlockedStream(sink: Sink[DataBuffer], continueTrigger: Trigger) extends InputState

  /**
   * The controller enters this state only during disconnects.  It indicates that we're no longer accepting new data
   */
  case object Terminated extends InputState
}


class InvalidInputStateException(state: InputState) extends Exception(s"Invalid Input State: $state")


/**
 * The InputController maintains all state dealing with reading in messages in
 * a controller.  It handles decoding messages and properly routing data into
 * stream
 */
trait InputController[Input, Output] extends MasterController[Input, Output] {
  import InputState._

  private[controller] var inputState: InputState = Decoding

  //we need to keep track of this outside the write-endpoint in case the endpoint changes
  //maybe not
  private var _readsEnabled = true
  def readsEnabled = _readsEnabled

  private[controller] def inputOnClosed() {
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

  private[controller] def inputOnConnected() {
    inputState = Decoding
  }

  private[controller] def inputGracefulDisconnect() {
    if (inputState == Decoding) {
      pauseReads()
      inputState = Terminated
    }
  }

  protected def pauseReads() {
    state match {
      case AliveState(endpoint) => {
        _readsEnabled = false
        endpoint.disableReads()
      }
    }
  }

  protected def resumeReads() {
    state match {
      case AliveState(endpoint) => {
        _readsEnabled = true
        endpoint.enableReads()
      }
    }
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
          case ConnectionState.Disconnecting(_) => {
            //gracefulDisconnect was called, so we allowed the connection to
            //finish reading in the stream, but now that it's done, disable
            //reads and drop any data still in the buffer
            pauseReads()
            inputState = Terminated
          }
          case ConnectionState.Connected(_) => {
            inputState = Decoding
            if(data.hasUnreadData) receivedData(data)
          }
          case other => throw new InvalidConnectionStateException(other)
        }
        case Success(Full(trigger)) => state match {
          case a: AliveState => {
            a.endpoint.disableReads()
            trigger.fill{() =>
              //todo: maybe do something if endpoint disappears before trigger is called.  Also maybe need to cancel trigger?
              resumeReads()
              inputState = ReadingStream(sink)
            }
            inputState = BlockedStream(sink, trigger)
          }
          case other => throw new InvalidConnectionStateException(other)
        }
        case Failure(a : PipeTerminatedException) => {
          //disconnect
          inputState = Terminated
          //we might not want to throw exceptions here, since terminating a pipe is normal behavior....maybe?
        }
        case Failure(a : PipeClosedException) => {
          //this only happens on infinite pipes...maybe also on finite pipes, but not yet
          //TODO:  this should not be an exception/failure
          inputState = Decoding
          if(data.hasUnreadData) receivedData(data)
        }
        case Failure(t : Throwable) => {
          inputState = Terminated
          throw t //basically gotta kill the connection
        }
      }
      case other => throw new InvalidInputStateException(other)
    }
  }


  protected def processMessage(message: Input)

}
