package colossus
package controller

import colossus.metrics.MetricNamespace
import colossus.parsing.DataSize
import colossus.parsing.DataSize._
import core._

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

//these are the methods that the controller layer requires to be implemented by it's downstream neighbor
trait ControllerDownstream[E <: Encoding] extends Upstream[ControllerUpstream]{

  def processMessage(input: E#Input)

  def onFatalError(reason: Throwable): Option[E#Output] = {
    //TODO: Logging
    println(s"Fatal Error: $reason, disconnecting")
    None
  }
}

trait Writer[T] {
  protected def push(item: T, createdMillis: Long = System.currentTimeMillis)(postWrite: QueuedItem.PostWrite): Boolean
  protected def canPush: Boolean
}

//these are the method that a controller layer itself must implement for its downstream neighbor
trait ControllerUpstream[E <: Encoding] extends Writer[E#Output] {
  protected def writesEnabled: Boolean
  protected def pauseWrites()
  protected def resumeWrites()
  protected def pauseReads()
  protected def resumeReads()
  protected def purgePending(reason: Throwable)
  protected def pendingBufferSize: Int
}

/**
 * methods that both input and output need but shouldn't be exposed in the above traits
 */
trait BaseController[E <: Encoding] {
  def fatalError(reason: Throwable) {
    onFatalError(reason).foreach{o => push(o){_ => ()}}
    disconnect()
  }

  def upstream: CoreUpstream
  def downstream: ControllerDownstream[E]
}

class Controller[E <: Encoding](context: Context, val downstream: ControllerDownstream, codec: Codec[E], controllerConfig: ControllerConfig) 
extends ControllerIface[E] with StaticInputController[E] with StaticOutputController[E] with CoreDownstream {
  
  downstream.setUpstream(this)
  

}




