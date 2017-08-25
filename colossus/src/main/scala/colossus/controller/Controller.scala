package colossus.controller

import colossus.core._
import colossus.metrics.MetricNamespace
import colossus.parsing.DataSize
import colossus.parsing.DataSize._
import colossus.streaming._

/**
  * Configuration for the controller
  *
  * @param outputBufferSize the maximum number of outbound messages that can be queued for sending at once
  * @param inputMaxSize maximum allowed input size (in bytes)
  */
case class ControllerConfig(
    outputBufferSize: Int,
    inputMaxSize: DataSize = 1.MB,
    metricsEnabled: Boolean = true
)

/**
  * Response type of controller fatal error handler.  This essentially instructs
  * the controller how to handle an unexpected error.
  */
sealed trait FatalErrorAction[+T]
object FatalErrorAction {

  /**
    * gracefully close the connection (generally used for server connections),
    * possibly sending a final message before closing.
    */
  case class Disconnect[T](lastMessage: Option[T]) extends FatalErrorAction[T]

  /**
    * Immediately terminate the connection (treated as a connection-level error)
    */
  case object Terminate extends FatalErrorAction[Nothing]
}

//these are the methods that the controller layer requires to be implemented by it's downstream neighbor
trait ControllerDownstream[E <: Encoding] extends HasUpstream[ControllerUpstream[E]] with DownstreamEvents {

  def incoming: Sink[E#Input]

  def onFatalError(reason: Throwable): FatalErrorAction[E#Output]

  def controllerConfig: ControllerConfig
  def namespace: MetricNamespace
}

//these are the method that a controller layer itself must implement for its downstream neighbor
trait ControllerUpstream[-E <: Encoding] extends UpstreamEvents {
  def connection: ConnectionManager
  def outgoing: Sink[E#Output]
}

/**
  * methods that both input and output need but shouldn't be exposed in the above traits
  */
trait BaseController[E <: Encoding]
    extends UpstreamEventHandler[CoreUpstream]
    with DownstreamEventHandler[ControllerDownstream[E]] {
  def fatalError(reason: Throwable, kill: Boolean)

  def controllerConfig: ControllerConfig
  def codec: Codec[E]
  def context: Context

  def incoming: Sink[E#Input]

  implicit val namespace: MetricNamespace
}

class Controller[E <: Encoding](val downstream: ControllerDownstream[E], val codec: Codec[E])
    extends ControllerUpstream[E]
    with StaticInputController[E]
    with StaticOutputController[E]
    with CoreDownstream {

  downstream.setUpstream(this)

  def connection         = upstream
  def controllerConfig   = downstream.controllerConfig
  implicit val namespace = downstream.namespace

  override def onConnectionTerminated(cause: DisconnectCause) {
    cause match {
      case error: DisconnectError => connectionLost(error)
      case other                  => connectionClosed(other)
    }
  }

  /**
    * Terminate the connection with an error.  If `forceKill` is true, the connection
    * will be immediately force-disconnected and treated as an error, otherwise
    * the downstream handler will have a chance to push a final message and
    * control through the returned [[FatalErrorAction]] how to close the
    * connection.
    *
    * (generally `forceKill` should be true when we know it's impossible to send
    * a message, such as when an error occurs in OutputController)
    * */
  def fatalError(reason: Throwable, forceKill: Boolean) {
    if (forceKill) {
      upstream.kill(reason)
    } else {
      downstream.onFatalError(reason) match {
        case FatalErrorAction.Disconnect(msgOpt) => {
          msgOpt.foreach { o =>
            outgoing.push(o)
          }
          upstream.disconnect()
        }
        case FatalErrorAction.Terminate => {
          upstream.kill(reason)
        }
      }
    }
  }

  val incoming = downstream.incoming

}

object Controller {

  def apply[E <: Encoding](downstream: ControllerDownstream[E] with HandlerTail, codec: Codec[E]): PipelineHandler = {
    new PipelineHandler(new Controller(downstream, codec), downstream)
  }
}
