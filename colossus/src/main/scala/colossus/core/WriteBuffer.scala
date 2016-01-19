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

  /**
   * The WriteBuffer calls this if it has been signaled to disconnect and
   * finishes writing any existing partial buffer
   */
  def completeDisconnect()

  /**
   * This should be called when it's time to disconnect the connection, but we
   * wish to finish writing any existing partial buffer.  We do this because any
   * levels higher up already consider any data in a partial buffer to be sent,
   * so we don't want to disconnect until we fullfil that promise.
   */
  def gracefulDisconnect() {
    disconnecting = true
    if (partialBuffer.isEmpty) {
      completeDisconnect()
    }
    //if the partial buffer is defined, completeDisconnect gets called when we
    //finish writing it
  }

  private var disconnecting = false

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
        if (!partialBuffer.isDefined) {
          //we must take a copy of the buffer since it will be repurposed.
          //notice if the partial buffer is defined, then it must be the same as
          //what we're currently trying to write, so we don't need to set it
          //again
          partialBuffer = Some(raw.takeCopy)
        }
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
    } else if (disconnecting) {
      Failed
    } else {
      writeRaw(raw)
    }
  }

  /**
   * Attempts to continue writing any existing partial buffer and returns true
   * if the write buffer is able to accept more data immediately.  This will
   * return false if the WriteBuffer is currently in the middle of draining an
   * existing PartialBuffer, so if this returns false, then calling `write` will
   * return `Zero`
   */
  private[colossus] def continueWrite(): Boolean = {
    partialBuffer.map{raw =>
      if (writeRaw(raw) == Complete) {
        if (disconnecting) {
          completeDisconnect()
        }
        true
      } else {
        false
      }
    }.getOrElse{
      true
    }
  }

  /**
   * returns true if more data can be written, false otherwise
   *
   * Note - the return value is only used in testing
   */
  protected def handleWrite(data: encoding.DataOutBuffer, handler: ConnectionHandler) = {
    if (continueWrite()) {
      //partial buffer is empty, so we can get data from the handler to write
      val more  = handler.readyForData(data)
      val toWrite = data.data
      //its possible for the handler to not actually have written anything, this
      //can occur due to the fact that we use the writeReady flag to track both
      //pending data from the handler and from the partial buffer, so we can end up
      //calling handler.readyForData even when it didn't request a write
      val result = if (toWrite.remaining > 0) write(toWrite) else WriteStatus.Complete
      //we want to leave writeReady enabled if either the handler has more data to write, or if the writebuffer couldn't write everything
      if (more == MoreDataResult.Complete && result == WriteStatus.Complete) {
        disableWriteReady()
      }
      true
    } else false
  }


  def requestWrite() {
    enableWriteReady()
  }

}

private[core] trait LiveWriteBuffer extends WriteBuffer {

  protected def channel: SocketChannel
  def channelWrite(raw: DataBuffer): Int = raw.writeTo(channel)
  def key: SelectionKey

  def setKeyInterest() {
    val ops = (if (readsEnabled) SelectionKey.OP_READ else 0) | (if (writeReadyEnabled) SelectionKey.OP_WRITE else 0)
    key.interestOps(ops)
  }


}
