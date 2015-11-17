package colossus
package controller

import core._
import service.Codec

import scala.concurrent.duration.Duration

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

/**
 * Configuration for the controller
 *
 * @param outputBufferSize the maximum number of outbound messages that can be queued for sending at once
 * @param sendTimeout if a queued outbound message becomes older than this it will be cancelled
 */
case class ControllerConfig(
  outputBufferSize: Int,
  sendTimeout: Duration
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
class InvalidConnectionStateException(state: ConnectionState) extends Exception(s"Invalid connection State: $state")



/**
 * The base trait inherited by both InputController and OutputController and
 * ultimately implemented by Controller.  This merely contains methods needed
 * by both input and output controller
 */
trait MasterController[Input, Output] extends ConnectionHandler {
  protected def state: ConnectionState
  protected def codec: Codec[Output, Input]
  protected def controllerConfig: ControllerConfig

  //needs to be called after various actions complete to check if it's ok to disconnect
  private[controller] def checkControllerGracefulDisconnect()

  //Right now in some cases the Input or Outut Controller decide to kill the
  //whole connection.  We should eventually make this configurable such that we
  //can kill one half of the controller without automatically killing the whole
  //thing
  def disconnect()
}



/**
 * A Controller is a Connection handler that is designed to work with
 * connections involving decoding raw bytes into input messages and encoding
 * output messages into bytes.
 *
 * Unlike a service, which pairs an input "request" message with an output
 * "response" message, the controller make no such pairing.  Thus a controller
 * can be thought of as a duplex stream of messages.
 */
abstract class Controller[Input, Output](val codec: Codec[Output, Input], val controllerConfig: ControllerConfig) 
extends InputController[Input, Output] with OutputController[Input, Output] {
  import ConnectionState._

  protected var state: ConnectionState = NotConnected
  def isConnected = state != NotConnected

  def connected(endpt: WriteEndpoint) {
    state match {
      case NotConnected => state = Connected(endpt)
      case other => throw new InvalidConnectionStateException(other)
    }
    codec.reset()
    outputOnConnected()
    inputOnConnected()
  }

  /**
   * Returns a read-only trait containing live information about the connection.
   */
  def connectionInfo: Option[ConnectionInfo] = state match {
    case a: AliveState => Some(a.endpoint)
    case _ => None
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

  def disconnect() {
    //this has to be public to be used for clients
    state match {
      case a: AliveState => {
        a.endpoint.disconnect()
      }
      case _ => {}
    }
  }

  /**
   * stops reading from the connection and accepting new writes, but waits for
   * pending/ongoing write operations to complete before disconnecting
   *
   * This is an idempotent operation
   */
  def gracefulDisconnect() {
    state match {
      case Connected(e) => {
        state = Disconnecting(e)
      }
      case Disconnecting(e) => {}
      case NotConnected => {}
    }
    inputGracefulDisconnect()
    outputGracefulDisconnect()
    checkControllerGracefulDisconnect()
  }

  private[controller] def checkControllerGracefulDisconnect() {
    (state, inputState, outputState) match {
      case (Disconnecting(endpoint), InputState.Terminated, OutputState.Terminated) => {
        endpoint.disconnect()
        state = NotConnected
      }
      case _ => {} 
    }
  }

}


