package colossus
package controller

import colossus.metrics.MetricNamespace
import colossus.parsing.DataSize
import colossus.parsing.DataSize._
import core._

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
  sendTimeout: Duration,
  inputMaxSize: DataSize = 1.MB,
  flushBufferOnClose: Boolean = true,
  metricsEnabled: Boolean = true
)

//these are the methods that the controller layer requires to be implemented by it's downstream neighbor
trait ControllerDownstream[E <: Encoding] extends HasUpstream[ControllerUpstream[E]]{

  def processMessage(input: E#Input)

  def onFatalError(reason: Throwable): Option[E#Output] = {
    //TODO: Logging
    println(s"Fatal Error: $reason, disconnecting")
    None
  }
}

trait Writer[T] {
  def push(item: T, createdMillis: Long = System.currentTimeMillis)(postWrite: QueuedItem.PostWrite): Boolean
  def canPush: Boolean
}

//these are the method that a controller layer itself must implement for its downstream neighbor
trait ControllerUpstream[E <: Encoding] extends Writer[E#Output] with UpstreamEvents {
  def writesEnabled: Boolean
  def pauseWrites()
  def resumeWrites()
  def pauseReads()
  def resumeReads()
  def purgePending(reason: Throwable)
  def pendingBufferSize: Int
  def connection: ConnectionManager
}

/**
 * methods that both input and output need but shouldn't be exposed in the above traits
 */
trait BaseController[E <: Encoding] { //self: Writer[E#Output] with HasUpstream[ConnectionManager] =>
  def fatalError(reason: Throwable) {
    //TODO: FIX
    //onFatalError(reason).foreach{o => push(o){_ => ()}}
    //upstream.disconnect()
  }

  def onConnected()

  def upstream: CoreUpstream
  def downstream: ControllerDownstream[E]
  def controllerConfig: ControllerConfig
  def codec: Codec[E]
  def context: Context

  implicit val namespace: MetricNamespace
}

class Controller[E <: Encoding](val context: Context, val downstream: ControllerDownstream[E], val codec: Codec[E], val controllerConfig: ControllerConfig) 
extends ControllerUpstream[E] with StaticInputController[E] with StaticOutputController[E] with CoreDownstream with UpstreamEventHandler[CoreUpstream] with DownstreamEventHandler[ControllerDownstream[E]]{

  //TODO : FIX - probably put this in controller config
  implicit val namespace: MetricNamespace = context.worker.system.metrics
  
  downstream.setUpstream(this)
  
  def connection = upstream
  

}




