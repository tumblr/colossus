package colossus
package controller

import colossus.metrics.{Histogram, MetricNamespace}
import scala.concurrent.duration._
import parsing.ParserSizeTracker

import core._
import service._


trait StaticCodec[I,O] {
  def decode(data: DataBuffer): Option[I]
  def encode(message: O, buffer: DataOutBuffer)

  def reset()
}
object StaticCodec {

  type Server[P <: Protocol] = StaticCodec[P#Input, P#Output]
  type Client[P <: Protocol] = StaticCodec[P#Output, P#Input]

  def wrap[I,O](s: Codec[O,I]): StaticCodec[I,O] = new StaticCodec[I,O] {
    def decode(input: DataBuffer): Option[I] = s.decode(input).map{
      case DecodedResult.Static(i) => i
      case _ => throw new Exception("not supported")
    }
    def encode(output: O, buffer: DataOutBuffer) {
      s.encode(output) match {
        case e: Encoder => {
          e.encode(buffer)
        }
        case _ => throw new Exception("Not supported")
      }
    }
    def reset() { s.reset() }
  }
}

//these are the methods that the controller layer requires to be implemented
trait ControllerIface[I,O] {
  protected def connectionState: ConnectionState
  protected def codec: StaticCodec[I,O]
  protected def processMessage(input: I)
  protected def controllerConfig: ControllerConfig
  implicit val namespace: MetricNamespace

  protected def onFatalError(reason: Throwable): Option[O] = None
}

//these are the method that a controller layer itself must implement
trait ControllerImpl[I,O] {
  protected def push(item: O, createdMillis: Long = System.currentTimeMillis)(postWrite: QueuedItem.PostWrite): Boolean
  protected def canPush: Boolean
  protected def purgePending(reason: Throwable)
  protected def writesEnabled: Boolean
  protected def pauseWrites()
  protected def resumeWrites()
  protected def pauseReads()
  protected def resumeReads()
}

/**
 * methods that both input and output need but shouldn't be exposed in the above traits
 */
trait BaseStaticController[I,O] extends CoreHandler with ControllerImpl[I,O]{this: ControllerIface[I,O] =>
  def fatalError(reason: Throwable) {
    onFatalError(reason).foreach{o => push(o){_ => ()}}
    disconnect()
  }
}

trait StaticController[I,O] extends StaticInputController[I,O] with StaticOutputController[I,O]{this: ControllerIface[I,O] => }



trait StaticInputController[I,O] extends BaseStaticController[I,O] {this: ControllerIface[I,O] =>
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

trait StaticOutputController[I,O] extends BaseStaticController[I,O]{this: ControllerIface[I,O] =>


  private var state: StaticOutState = StaticOutState.Suspended
  private def disconnecting = state.disconnecting
  private var _writesEnabled = true
  private var outputBuffer = new MessageQueue[O](controllerConfig.outputBufferSize)

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

  def push(item: O, createdMillis: Long = System.currentTimeMillis)(postWrite: QueuedItem.PostWrite): Boolean = {
    if (canPush) {
      if (outputBuffer.isEmpty) signalWrite()
      outputBuffer.enqueue(item, postWrite, createdMillis)
      true
    } else false
  }

  def readyForData(buffer: DataOutBuffer) =  {
    if (disconnecting && outputBuffer.isEmpty) {
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
