package colossus
package controller

import colossus.metrics.MetricNamespace
import colossus.parsing.DataSize
import colossus.parsing.DataSize._
import core._
import colossus.streaming._

import scala.concurrent.duration._

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
  inputMaxSize: DataSize = 1.MB,
  metricsEnabled: Boolean = true
)

//these are the methods that the controller layer requires to be implemented by it's downstream neighbor
trait ControllerDownstream[E <: Encoding] extends HasUpstream[ControllerUpstream[E]] with DownstreamEvents {

  def incoming: Sink[E#Input]

  def onFatalError(reason: Throwable): Option[E#Output] = {
    //TODO: Logging
    println(s"Fatal Error: $reason, disconnecting")
    reason.printStackTrace()
    None
  }

  def controllerConfig: ControllerConfig
}

//these are the method that a controller layer itself must implement for its downstream neighbor
trait ControllerUpstream[-E <: Encoding] extends UpstreamEvents {
  def connection: ConnectionManager
  def outgoing: Sink[E#Output]
}

/**
 * methods that both input and output need but shouldn't be exposed in the above traits
 */
trait BaseController[E <: Encoding] extends UpstreamEventHandler[CoreUpstream] with DownstreamEventHandler[ControllerDownstream[E]] { 
  def fatalError(reason: Throwable) 

  def controllerConfig: ControllerConfig
  def codec: Codec[E]
  def context: Context

  def incoming: Sink[E#Input]

  implicit val namespace: MetricNamespace
}

class Controller[E <: Encoding](val downstream: ControllerDownstream[E], val codec: Codec[E]) 
extends ControllerUpstream[E] with StaticInputController[E] with StaticOutputController[E] with CoreDownstream {

  //TODO : FIX - probably put this in controller config
  implicit val namespace: MetricNamespace = context.worker.system.metrics
  
  downstream.setUpstream(this)
  
  def connection = upstream
  def controllerConfig = downstream.controllerConfig

  override def onConnectionTerminated(cause: DisconnectCause) {
    cause match {
      case error: DisconnectError => connectionLost(error)
      case other => connectionClosed(other)
    }
  }

  def fatalError(reason: Throwable) {
    //TODO: FIX
    downstream.onFatalError(reason).foreach{o => outgoing.push(o)}
    upstream.disconnect()
  }

  val incoming = downstream.incoming
  

}

object Controller {

  def apply[E <: Encoding](downstream: ControllerDownstream[E] with HandlerTail, codec: Codec[E]): PipelineHandler = {
    new PipelineHandler(new Controller(downstream, codec), downstream)
  }
}


