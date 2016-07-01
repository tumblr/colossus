package colossus
package controller

import colossus.metrics.{Histogram, MetricNamespace}
import scala.concurrent.duration._
import parsing.ParserSizeTracker

import core._
import service._



//these are the methods that the controller layer requires to be implemented
trait ControllerIface[E <: Encoding] {
  protected def connectionState: ConnectionState
  protected def codec: StaticCodec[E]
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




