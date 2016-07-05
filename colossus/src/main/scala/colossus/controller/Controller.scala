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

//these are the methods that the controller layer requires to be implemented
trait ControllerIface[E <: Encoding] {
  protected def connectionState: ConnectionState
  protected def codec: Codec[E]
  protected def processMessage(input: E#Input)
  protected def controllerConfig: ControllerConfig
  implicit val namespace: MetricNamespace

  protected def onFatalError(reason: Throwable): Option[E#Output] = None
}

//these are the method that a controller layer itself must implement
trait ControllerImpl[E <: Encoding] {
  protected def push(item: E#Output, createdMillis: Long = System.currentTimeMillis)(postWrite: QueuedItem.PostWrite): Boolean
  protected def canPush: Boolean
  protected def purgePending(reason: Throwable)
  protected def writesEnabled: Boolean
  protected def pauseWrites()
  protected def resumeWrites()
  protected def pauseReads()
  protected def resumeReads()
  protected def pendingBufferSize: Int
}

/**
 * methods that both input and output need but shouldn't be exposed in the above traits
 */
trait BaseStaticController[E <: Encoding] extends CoreHandler with ControllerImpl[E]{this: ControllerIface[E] =>
  def fatalError(reason: Throwable) {
    onFatalError(reason).foreach{o => push(o){_ => ()}}
    disconnect()
  }
}

trait StaticController[E <: Encoding] extends StaticInputController[E] with StaticOutputController[E]{this: ControllerIface[E] => }

/**
 * This can be used to build connection handlers directly on top of the
 * controller layer
 */
abstract class BasicController[E <: Encoding](
  val codec: Codec[E],
  val controllerConfig: ControllerConfig,
  val context: Context
) extends StaticController[E] { self: ControllerIface[E] => }


