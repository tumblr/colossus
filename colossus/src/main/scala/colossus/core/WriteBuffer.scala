package colossus
package core

import java.nio.ByteBuffer
import java.nio.channels.{CancelledKeyException, ClosedChannelException, SelectionKey, SocketChannel}

sealed trait WriteStatus
object WriteStatus {
  //connection is busted
  case object Failed extends WriteStatus
  //data was partially written and the rest is buffered
  case object Partial extends WriteStatus
  //buffered data is still being written, requested write did not occur
  case object Zero extends WriteStatus
  //all the data was written
  case object Complete extends WriteStatus
}

trait KeyInterestManager {
  private var _readsEnabled = true
  private var _writeReadyEnabled = false

  def readsEnabled = _readsEnabled
  def writeReadyEnabled = _writeReadyEnabled

  protected def setKeyInterest()

  def enableReads() {
    _readsEnabled = true
    setKeyInterest()
  }
  def disableReads() {
    _readsEnabled = false
    setKeyInterest()
  }
  def enableWriteReady() {
    _writeReadyEnabled = true
    setKeyInterest()
  }

  def disableWriteReady() {
    _writeReadyEnabled = false
    setKeyInterest()
  }
}

private[colossus] trait WriteBuffer extends KeyInterestManager {
  import WriteStatus._

  //mostly for DI for testing
  def channelWrite(data: DataBuffer): Int

  private var _bytesSent = 0L
  def bytesSent = _bytesSent

  //this is only filled when we only partially wrote data
  private var partialBuffer: Option[DataBuffer] = None

  //technically this value is wrong when first constructed, but since this is
  //only used in determining idle time, initializing it to the current time
  //simplifies the calculations
  private var _lastTimeDataWritten: Long = System.currentTimeMillis

  def lastTimeDataWritten = _lastTimeDataWritten

  def isDataBuffered: Boolean = partialBuffer.isDefined

  private def writeRaw(raw: DataBuffer): WriteStatus = {
    try {
      _bytesSent += channelWrite(raw)
      _lastTimeDataWritten = System.currentTimeMillis
      if (raw.hasUnreadData) {
        //we must take a copy of the buffer since it will be repurposed
        partialBuffer = Some(raw.takeCopy)
        Partial
      } else {
        partialBuffer = None
        Complete
      }
    } catch {
      case t: CancelledKeyException => {
        //no cleanup is required since the connection is closed for good, 
        Failed
      }
    }
  }

  def write(raw: DataBuffer): WriteStatus = {
    if (partialBuffer.isDefined) {
      Zero
    } else {
      writeRaw(raw)
    }
  }

  def continueWrite(): Boolean {
    partialBuffer.map{raw =>
      if (writeRaw(raw) == Complete) {
        true
      } else {
        false
      }
    }.getOrElse{
      true
    }
  }
}

private[core] trait LiveWriteBuffer extends WriteBuffer {

  //DO NOT MAKE THIS A VAL, screws up initialization order
  def internalBufferSize = 1024 * 64

  protected def channel: SocketChannel
  def channelWrite(raw: DataBuffer): Int = raw.writeTo(channel)
  def key: SelectionKey

  def setKeyInterest() {
    val ops = (if (readsEnabled) SelectionKey.OP_READ else 0) | (if (writeReadyEnabled) SelectionKey.OP_WRITE else 0)
    key.interestOps(ops)
  }


}
