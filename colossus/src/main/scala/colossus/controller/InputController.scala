package colossus
package controller

import core._
import colossus.service.{DecodedResult, NotConnectedException}

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
 *
 * When pushing data into a stream, the controller has some very specific behavior regarding the PushResult from the stream.
 * - Terminating the stream at any point kills the connection
 * - Closing a stream outside of a pull callback will kill the connection without logging the error
 * - Closing a stream inside of a pull callback will complete the stream and the controller will resset
 * 
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
      case Decoding => {
        //this only occurs if a codec depends on the connection closing to
        //know when a message is fully received (eg http with no
        //content-length)
        codec.endOfStream().foreach{
          case DecodedResult.Static(fin) => processMessage(fin)
          case DecodedResult.Stream(fin, sink) => {
            processMessage(fin)
            sink.terminate(new NotConnectedException("Connection Closed"))
          }
        }
      }
      case ReadingStream(sink) => {
        sink.terminate(new NotConnectedException("Connection Closed"))
      }
      case BlockedStream(sink, trigger) => {
        sink.terminate(new NotConnectedException("Connection Closed"))
        trigger.cancel()
      }
      case _ => {}
    }
    inputState = Terminated
  }

  private[controller] def inputOnConnected() {
    _readsEnabled = true
    resumeReads()
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
      case a : AliveState => {
        _readsEnabled = false
        a.endpoint.disableReads()
      }
      case _ => {}
    }
  }

  protected def resumeReads() {
    state match {
      case a: AliveState => {
        _readsEnabled = true
        a.endpoint.enableReads()
      }
      case _ => {}
    }
  }


  def receivedData(data: DataBuffer) {

    def processAndContinue(msg : Input, buffer : DataBuffer) {
      processMessage(msg)
      if(buffer.hasUnreadData) receivedData(buffer)
    }

    inputState match {
      case Decoding => {
        var decoding = true
        while (decoding) {
          codec.decode(data) match {
            case Some(DecodedResult.Static(msg)) => {
              processMessage(msg)
            }
            case None => {
              decoding = false
            } 
            case Some(DecodedResult.Stream(msg, sink)) => {
              decoding = false
              inputState = ReadingStream(sink)
              processAndContinue(msg, data)
            }
          }
        }
      }
      case ReadingStream(sink) => sink.push(data) match {
        case PushResult.Ok => {}
        case PushResult.Complete => state match{
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
        case PushResult.Full(trigger) => state match {
          case a: AliveState => {
            a.endpoint.disableReads()
            val copied = data.takeCopy
            trigger.fill{() =>
              resumeReads()
              inputState = ReadingStream(sink)
              receivedData(copied)
            }
            inputState = BlockedStream(sink, trigger)
          }
          case other => throw new InvalidConnectionStateException(other)
        }
        case PushResult.Filled(trigger) => state match {
          case a: AliveState => {
            a.endpoint.disableReads()
            trigger.fill{() => 
              resumeReads()
              inputState = ReadingStream(sink)
            }
          }
          case other => throw new InvalidConnectionStateException(other)
        }
        case PushResult.Closed => {
          //TODO: This result indicates the receiver closed the pipe early,
          //since otherwise we already would have gotten the Complete state.
          //So this result would only happen if the receiever expected more data, but abruptly closed the pipe.
          //Seems like the only solution here is to close the connection since
          //we no longer have any place to put this data.  But this might need more thought
          disconnect()
        }
        case PushResult.Error(t : Throwable) => {
          inputState = Terminated
          throw t //basically gotta kill the connection
        }
      }
      case other => throw new InvalidInputStateException(other)
    }
  }


  protected def processMessage(message: Input)

}
