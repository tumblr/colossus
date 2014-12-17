package colossus
package controller

import core._
import service.Codec

/** trait representing a decoded message that is actually a stream
 * 
 * When a codec decodes a message that contains a stream (perhaps an http
 * request with a very large body), the returned message MUST extend this
 * trait, otherwise the InputController will not know to push subsequent data
 * to the stream message and things will get all messed up. 
 */
trait StreamMessage {
  def source: Source[DataBuffer]
}

trait MessageHandler[Input, Output] {
  def codec: Codec[Output, Input]
  def controllerConfig: ControllerConfig
}

case class ControllerConfig(
  outputBufferSize: Int
)

abstract class Controller[Input, Output](val codec: Codec[Output, Input], val controllerConfig: ControllerConfig) 
extends InputController[Input, Output] with OutputController[Input, Output] {

  protected var writer: Option[WriteEndpoint] = None

  def connected(endpoint: WriteEndpoint) {
    if (writer.isDefined) {
      throw new Exception("Handler Connected twice!")
    }
    writer = Some(endpoint)
  }
}


