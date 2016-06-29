package colossus
package controller

import colossus.metrics.{Histogram, MetricNamespace}
import scala.concurrent.duration._
import parsing.ParserSizeTracker

import core._
import service._


trait StaticCodec[E <: Encoding] {
  def decode(data: DataBuffer): Option[E#Input]
  def encode(message: E#Output, buffer: DataOutBuffer)

  //TODO use this in the controller
  def endOfStream(): Option[E#Input]

  def reset()

  def decodeAll(data: DataBuffer)(onDecode : E#Input => Unit) { if (data.hasUnreadData){
    var done: Option[E#Input] = None
    do {
      done = decode(data)
      done.foreach{onDecode}
    } while (done.isDefined && data.hasUnreadData)
  }}
}

case class StreamInput[I](sink: Sink[DataBuffer], message: I)
trait StreamCodec[E <: Encoding] {
  def decode(data: DataBuffer): Option[StreamInput[E#Input]]
  def encode(message: E#Output): Source[DataBuffer]
}

object StaticCodec {

  type Server[P <: Protocol] = StaticCodec[P#ServerEncoding]
  type Client[P <: Protocol] = StaticCodec[P#ClientEncoding]

}

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



trait StaticInputController[E <: Encoding] extends BaseStaticController[E] {this: ControllerIface[E] =>
  private var _readsEnabled = true
  def readsEnabled = _readsEnabled

  //this has to be lazy to avoid initialization-order NPE
  lazy val inputSizeHistogram = if (controllerConfig.metricsEnabled) {
    Some(Histogram("input_size", sampleRate = 0.10, percentiles = List(0.75,0.99)))
  } else {
    None
  }
  lazy val inputSizeTracker = new ParserSizeTracker(Some(controllerConfig.inputMaxSize), inputSizeHistogram)

  def pauseReads() {
    connectionState match {
      case a : AliveState => {
        _readsEnabled = false
        a.endpoint.disableReads()
      }
      case _ => {}
    }
  }

  def resumeReads() {
    connectionState match {
      case a: AliveState => {
        _readsEnabled = true
        a.endpoint.enableReads()
      }
      case _ => {}
    }
  }

  def receivedData(data: DataBuffer) {
    try {
      while (data.hasUnreadData) {
        inputSizeTracker.track(data)(codec.decode(data)) match {
          case Some(msg) => processMessage(msg)
          case None => {}
        }
      }
    } catch {
      case reason: Throwable => {
        fatalError(reason)
      }
    }
  }

  override def connected(endpt: WriteEndpoint) {
    super.connected(endpt)
    codec.reset()
  }

}

abstract class StaticOutState(val canPush: Boolean) {
  def disconnecting = !canPush
}
object StaticOutState {
  case object Suspended extends StaticOutState(true)
  case object Alive extends StaticOutState(true)
  case object Disconnecting extends StaticOutState(false)
  case object Terminated extends StaticOutState(false)
}

trait StaticOutputController[E <: Encoding] extends BaseStaticController[E]{this: ControllerIface[E] =>


  private var state: StaticOutState = StaticOutState.Suspended
  private def disconnecting = state.disconnecting
  private var _writesEnabled = true
  private var outputBuffer = new MessageQueue[E#Output](controllerConfig.outputBufferSize)

  protected def pendingBufferSize = outputBuffer.size

  def writesEnabled = _writesEnabled

  def pauseWrites() {
    _writesEnabled = false
  }

  /**
   * Resumes writing of messages if currently paused, otherwise has no affect
   */
  def resumeWrites() {
    _writesEnabled = true
    if (!outputBuffer.isEmpty) signalWrite()
  }

  protected def purgePending(reason: Throwable) {
    while (!outputBuffer.isEmpty) {
      outputBuffer.dequeue.postWrite(OutputResult.Cancelled(reason))
    }

  }

  override def connected(endpt: WriteEndpoint) {
    super.connected(endpt)
    state = StaticOutState.Alive
    if (!outputBuffer.isEmpty) signalWrite()
  }


  private def onClosed() {
    if (disconnecting) {
      val reason = new NotConnectedException("Connection Closed")
      purgePending(reason)
    } 
  }

  protected def connectionClosed(cause : DisconnectCause) {
    state = StaticOutState.Terminated
    onClosed()
  }

  protected def connectionLost(cause : DisconnectError) {
    state = StaticOutState.Suspended
    onClosed()
  }

  def idleCheck(period: Duration) {
    val time = System.currentTimeMillis
    while (!outputBuffer.isEmpty && outputBuffer.head.isTimedOut(time, controllerConfig.sendTimeout)) {
      val expired = outputBuffer.dequeue
      expired.postWrite(OutputResult.Cancelled(new RequestTimeoutException))
    }
  }

  private def signalWrite() {
    connectionState match {
      case a: AliveState => {
        if (writesEnabled) a.endpoint.requestWrite()
      }
      case _ => {}
    }
  }

  override def shutdown() {
    state = StaticOutState.Disconnecting
    checkShutdown()
  }

  private def checkShutdown() {
    if (disconnecting && outputBuffer.isEmpty) {
      super.shutdown()
    }
  }


  def canPush = state.canPush && !outputBuffer.isFull

  def push(item: E#Output, createdMillis: Long = System.currentTimeMillis)(postWrite: QueuedItem.PostWrite): Boolean = {
    if (canPush) {
      if (outputBuffer.isEmpty) signalWrite()
      outputBuffer.enqueue(item, postWrite, createdMillis)
      true
    } else false
  }

  def readyForData(buffer: DataOutBuffer) =  {
    if (disconnecting && outputBuffer.isEmpty) {
      checkShutdown()
      MoreDataResult.Complete
    } else {
      while (writesEnabled && outputBuffer.size > 0 && ! buffer.isOverflowed) {
        val next = outputBuffer.dequeue
        codec.encode(next.item, buffer)
        next.postWrite(OutputResult.Success)
      }
      if (disconnecting || (writesEnabled && outputBuffer.size > 0)) {
        //return incomplete only if we overflowed the buffer and have more in
        //the queue, or if there's nothing left but we're disconnecting to
        //finish the disconnect process
        MoreDataResult.Incomplete
      } else {
        MoreDataResult.Complete
      }
    }
  }
        
      

  

}
