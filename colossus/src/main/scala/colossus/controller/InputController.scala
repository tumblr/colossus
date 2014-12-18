package colossus
package controller

import scala.util.{Try, Success, Failure}
import core._


/**
 * A mixin for ConnectionHandler that takes control of all read operations.
 * This will properly handle decoding messages, and for messages which are
 * streams, it will properly route data into the stream's source and handle
 * backpressure.
 */
trait InputController[Input, Output] extends ConnectionHandler with MessageHandler[Input, Output] {
  import PushResult._

  //todo: read-endpoint

  private var currentSink: Option[Sink[DataBuffer]] = None
  private var currentTrigger: Option[Trigger] = None

  def receivedData(data: DataBuffer) {
    currentSink match {
      case Some(source) => source.push(data) match {
        case Success(Ok) => {}
        case Success(Done) => {
          currentSink = None
          //recurse since the databuffer may still contain data for the next request
          if (data.hasUnreadData) receivedData(data)
        }
        case Success(Full(trigger)) => {
          //TODO: disconnect reads and set trigger to re-enable reads
          currentTrigger = Some(trigger)
        }
        case Failure(reason) => {
          //todo: what to do here?
        }
      }
      case None => codec.decode(data) match {
        case Some(message) =>  {
          message match {
            case s: StreamMessage => {
              currentSink = Some(s.sink)
            }
            case _ => {}
          }
          processMessage(message)
          if (data.hasUnreadData) receivedData(data)
        }
        case None => {}
      }
    }
  }


  protected def processMessage(message: Input)

}
