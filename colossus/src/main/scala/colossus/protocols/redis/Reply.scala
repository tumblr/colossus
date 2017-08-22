package colossus.protocols.redis

import colossus.core.DataBuffer

import akka.util.{ByteString, ByteStringBuilder}

import UnifiedProtocol._
import UnifiedBuilder._

sealed trait Reply {
  def raw: DataBuffer
}

sealed abstract class MessageReply(message: String) extends Reply {
  def prefix: ByteString

  def raw = {
    val b = new ByteStringBuilder
    b.sizeHint(message.size + 3)
    b append prefix
    b putBytes message.getBytes
    b append RN
    DataBuffer(b.result)
  }
}

case class StatusReply(message: String) extends MessageReply(message) {
  def prefix            = STATUS_REPLY
  override def toString = "StatusReply(" + message + ")"
}

case class ErrorReply(message: String) extends MessageReply(message) {
  def prefix            = ERROR_REPLY
  override def toString = "ErrorReply(" + message + ")"
}

case class IntegerReply(value: Long) extends Reply {
  def raw = DataBuffer(INTEGER_REPLY ++ ByteString(value.toString) ++ RN)
}
case class BulkReply(data: ByteString) extends Reply {
  def raw = {
    val b = new ByteStringBuilder
    buildArg(data, b)
    DataBuffer(b.result)
  }
  override def toString = "BulkReply(" + data.utf8String + ")"
}
case class MBulkReply(replies: Seq[Reply]) extends Reply {
  def raw = {
    val b = new ByteStringBuilder
    b append MBULK_REPLY
    b append ByteString(replies.size.toString)
    b append RN
    replies.foreach { reply =>
      b append ByteString(reply.raw.takeAll)
    } //TODO no raw data access!
    DataBuffer(b.result)
  }
}

case object EmptyMBulkReply extends Reply {
  def raw = DataBuffer(EMPTY_MBULK_REPLY)
}

case object NilReply extends Reply {
  def raw = DataBuffer(NIL_REPLY)
}

case object NilMBulkReply extends Reply {
  def raw = DataBuffer(NIL_MBULK_REPLY)
}
