package colossus
package controller

import scala.concurrent.duration._

import core._
import service._

trait StaticCodec[P <: Protocol] {
  def decode(data: DataBuffer): Option[P#Input]
  def encode(message: P#Output, buffer: DataOutBuffer)

  def reset()
}
object StaticCodec {
  def wrap[P <: Protocol](s: Codec.ServerCodec[P#Input,P#Output]): StaticCodec[P] = new StaticCodec[P] {
    def decode(input: DataBuffer): Option[P#Input] = s.decode(input).map{
      case DecodedResult.Static(i) => i
      case _ => throw new Exception("not supported")
    }
    def encode(output: P#Output, buffer: DataOutBuffer) {
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
trait ControllerIface[P <: Protocol] {
  protected def connectionState: ConnectionState
  def codec: StaticCodec[P]
  protected def processMessage(input: P#Input)
  def controllerConfig: ControllerConfig
}

//these are the method that a controller layer itself must implement
trait ControllerImpl[P <: Protocol] {
  def push(item: P#Output, createdMillis: Long = System.currentTimeMillis)(postWrite: QueuedItem.PostWrite): Boolean
  def canPush: Boolean
  protected def purgePending()
  def writesEnabled: Boolean
  def pauseWrites()
  def resumeWrites()
  def pauseReads()
  def resumeReads()
}

trait StaticController[P <: Protocol] extends StaticInputController[P] with StaticOutputController[P]{this: ControllerIface[P] => }


trait StaticInputController[P <: Protocol] extends CoreHandler with ControllerImpl[P]{this: ControllerIface[P] =>
  private var _readsEnabled = true
  def readsEnabled = _readsEnabled

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
    while (data.hasUnreadData) {
      codec.decode(data) match {
        case Some(msg) => processMessage(msg)
        case None => {}
      }
    }
  }

  override def connected(endpt: WriteEndpoint) {
    super.connected(endpt)
    codec.reset()
  }

}

trait StaticOutputController[P <: Protocol] extends CoreHandler with ControllerImpl[P]{this: ControllerIface[P] =>


  private var disconnecting = false
  private var _writesEnabled = true
  private var outputBuffer = new MessageQueue[P#Output](controllerConfig.outputBufferSize)

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

  protected def purgePending() {
    val reason = new NotConnectedException("Connection Closed")
    while (!outputBuffer.isEmpty) {
      outputBuffer.dequeue.postWrite(OutputResult.Cancelled(reason))
    }

  }



  private def onClosed() {
    if (disconnecting) {
      purgePending
    }
  }

  def connectionClosed(cause : DisconnectCause) {
    onClosed()
  }

  def connectionLost(cause : DisconnectError) {
    onClosed()
  }

  def idleCheck(period: Duration) {}

  private def signalWrite() {
    connectionState match {
      case a: AliveState => {
        if (writesEnabled) a.endpoint.requestWrite()
      }
      case _ => {}
    }
  }


  //TODO: connectionstate
  def canPush = !outputBuffer.isFull

  def push(item: P#Output, createdMillis: Long = System.currentTimeMillis)(postWrite: QueuedItem.PostWrite): Boolean = {
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
