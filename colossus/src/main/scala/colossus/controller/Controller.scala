package colossus
package controller

import colossus.parsing.DataSize
import colossus.parsing.DataSize._
import core._
import service.Codec

import scala.concurrent.duration.Duration

/**
 * Configuration for the controller
 *
 * @param outputBufferSize the maximum number of outbound messages that can be queued for sending at once
 * @param sendTimeout if a queued outbound message becomes older than this it will be cancelled
 * @param inputMaxSize maximum allowed input size (in bytes)
 * @param flushBufferOnClose
 */
case class ControllerConfig(
  outputBufferSize: Int,
  sendTimeout: Duration,
  inputMaxSize: DataSize = 1.MB,
  flushBufferOnClose: Boolean = true,
  metricsEnabled: Boolean = true
)

//used to terminate input streams when a connection is closing
class DisconnectingException(message: String) extends Exception(message)





/**
 * The base trait inherited by both InputController and OutputController and
 * ultimately implemented by Controller.  This merely contains methods needed
 * by both input and output controller
 */
trait MasterController[Input, Output] extends ConnectionHandler with IdleCheck {
  protected def connectionState: ConnectionState
  protected def codec: Codec[Output, Input]
  protected def controllerConfig: ControllerConfig

  implicit def namespace : metrics.MetricNamespace

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
abstract class Controller[Input, Output](val codec: Codec[Output, Input], val controllerConfig: ControllerConfig, context: Context)
extends CoreHandler(context) with InputController[Input, Output] with OutputController[Input, Output] {
  import ConnectionState._


  override def connected(endpt: WriteEndpoint) {
    super.connected(endpt)
    codec.reset()
    outputOnConnected()
    inputOnConnected()
  }


  private def onClosed() {
    inputOnClosed()
    outputOnClosed()
  }

  protected def connectionClosed(cause : DisconnectCause) {
    onClosed()
  }

  protected def connectionLost(cause : DisconnectError) {
    onClosed()
  }

  override def shutdown() {
    inputGracefulDisconnect()
    outputGracefulDisconnect()
    checkControllerGracefulDisconnect()
  }

  def fatalInputError(reason: Throwable) = {

    processBadRequest(reason).foreach { output =>
      push(output) { _ => {} }
    }
    disconnect()
    //throw reason
  }

  /**
   * Checks the status of the shutdown procedure.  Only when both the input and
   * output sides are terminated do we call shutdownRequest on the parent
   */
  private[controller] def checkControllerGracefulDisconnect() {
    (connectionState, inputState, outputState) match {
      case (ShuttingDown(endpoint), InputState.Terminated, OutputState.Terminated) => {
        super.shutdown()
      }
      case (NotConnected, _, _) => {
        //can happen when disconnect is called before being connected
        super.shutdown()
      }
      case _ => {}
    }
  }

}


