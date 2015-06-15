package colossus
package core

import java.nio.ByteBuffer
import java.nio.channels.{ClosedChannelException, SelectionKey, SocketChannel}

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

  def internalBufferSize: Int

  //this will be called whenever a partial buffer was fully written from and handleWrite
  def onBufferClear(): Unit

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

  //this is set to true when we are in the process of writing the internal
  //buffer to the channel.  Normally this is only true outside of handleWrite
  //when we fail to write the whole internal buffer to the socket
  private var drainingInternal = false

  //this is only used when the connection is about to disconnect.  We allow the
  //write buffer to drain, then perform the actual disconnect.  Once this is
  //set, no more writes are allowed and the connection is considered severed
  //from the user's point of view
  //
  //TODO: this is a total hack, make it less so
  private var disconnectCallback: Option[() => Unit] = None
  def disconnectBuffer(cb: () => Unit) {
    if (partialBuffer.isEmpty && drainingInternal == false && internal.data.position == 0) {
      cb()
      //we set this to prevent any further writes (see write())
      disconnectCallback = Some(() => ())
    } else {
      disconnectCallback = Some(cb)
    }
  }


  def isDataBuffered: Boolean = partialBuffer.isDefined

  //all writes are initially written to this internal buffer.  The buffer is
  //then drained at most once per event loop.  This ends up being much faster
  //than attempting to directly write to the socket each time
  private val internal = DataBuffer(ByteBuffer.allocateDirect(internalBufferSize))

  //copy as much data as possible from src into the internal buffer
  private def copyInternal(src: ByteBuffer) {
    val oldLimit = src.limit()
    val newLimit = if (src.remaining > internal.remaining) {
      oldLimit - (src.remaining - internal.remaining)
    } else {
      oldLimit
    }
    src.limit(newLimit)
    internal.data.put(src)
    src.limit(oldLimit)
  }

  private def writeRaw(raw: DataBuffer): WriteStatus = {
    _lastTimeDataWritten = System.currentTimeMillis
    enableWriteReady()
    copyInternal(raw.data)
    if (raw.hasUnreadData) {
      //we must take a copy of the buffer since it will be repurposed
      partialBuffer = Some(raw.takeCopy)
      Partial
    } else {
      partialBuffer = None
      Complete
    }
  }

  //this method is designed such that the caller can safely call it once and not
  //have to worry about having its data rejected.  This way the caller doesn't
  //need to do any buffering of its own, though it does need to be aware that
  //any subsequent calls will return a Zero write status
  def write(raw: DataBuffer): WriteStatus = {
    if (disconnectCallback.isDefined) {
      Failed
    } else if (partialBuffer.isDefined) {
      Zero
    } else {
      writeRaw(raw)
    }
  }

  //called whenever we're subbed to OP_WRITE
  def handleWrite() {
    //write data from the internal buffer
    if (!drainingInternal) {
      drainingInternal = true
      internal.data.flip //prepare for reading
    }
    val wrote = channelWrite(internal)
    //println(s"wrote $wrote")
    _bytesSent += wrote
    if (internal.remaining == 0) {
      //hooray! we wrote all the data, now we can accept more
      internal.data.clear()
      disableWriteReady()
      drainingInternal = false
    }

    partialBuffer.map{raw =>
      if (writeRaw(raw) == Complete) {
        //notice that onBufferClear is only called if the user had previously
        //called write and we returned a Partial status (which would result in
        //partialBuffer being set)
        onBufferClear()
      }
    }.getOrElse{
      disconnectCallback.foreach{cb => cb()}
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
