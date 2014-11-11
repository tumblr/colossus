package colossus
package core

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

private[colossus] trait WriteBuffer {
  import WriteStatus._

  //this will be called whenever a partial buffer was fully written from and handleWrite
  def onBufferClear(): Unit

  //mostly for DI for testing
  def channelWrite(data: DataBuffer): Int
  def keyInterestReadWrite(): Unit
  def keyInterestReadOnly(): Unit

  private var _bytesSent = 0L
  def bytesSent = _bytesSent


  //this is only filled when we only partially wrote data
  private var partialBuffer: Option[DataBuffer] = None

  def isDataBuffered: Boolean = partialBuffer.isDefined

  private def writeRaw(raw: DataBuffer): WriteStatus = {
    try {
      val wrote = channelWrite(raw)
      _bytesSent += wrote
      if (raw.hasUnreadData) {
        //we must take a copy of the buffer since it will be repurposed
        partialBuffer = Some(raw.takeCopy)
        keyInterestReadWrite()
        Partial
      } else {
        partialBuffer = None
        Complete
      }
    } catch {
      case c: ClosedChannelException => {
        Failed
      }
      case i: java.io.IOException => {
        Failed
      }
    }
  }

  def write(raw: DataBuffer): WriteStatus = {
    val p = partialBuffer.map{writeRaw}.getOrElse(Complete)
    if (p == Complete) {
      writeRaw(raw)
    } else {
      Zero
    }
  }

  //called whenever we're subbed to OP_WRITE
  def handleWrite() {
    partialBuffer.map{raw =>
      //trace(s"writing ${raw.size} partial")
      if (writeRaw(raw) == Complete) {
        onBufferClear()
      }
    }

    if (!partialBuffer.isDefined) {
      keyInterestReadOnly()
    }
  }
}

private[core] trait LiveWriteBuffer extends WriteBuffer {
  def key: SelectionKey
  protected def channel: SocketChannel

  def channelWrite(raw: DataBuffer): Int = raw.writeTo(channel)

  def keyInterestReadWrite() {
    key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE)
  }
  def keyInterestReadOnly() {
    key.interestOps(SelectionKey.OP_READ)
  }
}
