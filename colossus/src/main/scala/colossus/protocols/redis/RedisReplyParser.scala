package colossus
package protocols.redis


import akka.util.ByteString
import colossus.parsing._


object RedisReplyParser {
  import Combinators._

  def apply(): Parser[Reply] = reply


  def reply: Parser[Reply] = byte |> {
    case BULK_BYTE    => bulkReply
    case ERROR_BYTE   => errorReply
    case STATUS_BYTE  => statusReply
    case MBULK_BYTE   => mBulkReply
    case INTEGER_BYTE => integerReply
  }

  protected def bulkReply = intUntil('\r') <~ byte |> {
    case -1 => const(NilReply)
    case n  => bytes(n.toInt) <~ bytes(2) >> {b => BulkReply(ByteString(b))}
  }

  protected def statusReply = stringReply >> {s => StatusReply(s)}

  protected def errorReply = stringReply >> {s => ErrorReply(s)}

  protected def integerReply = intUntil('\r') <~ byte >> {i => IntegerReply(i)}

  protected def stringReply = stringUntil('\r', allowWhiteSpace = true) <~ byte

  protected def mBulkReply = intUntil('\r') <~ byte |> {
    case 0 => const(EmptyMBulkReply)
    case -1 => const(NilMBulkReply)
    case n => repeat(n.toInt, reply) >> {replies => MBulkReply(replies)}
  }


  val BULK_BYTE: Byte    = ByteString("$")(0)
  val ERROR_BYTE: Byte   = ByteString("-")(0)
  val STATUS_BYTE: Byte  = ByteString("+")(0)
  val MBULK_BYTE: Byte   = ByteString("*")(0)
  val INTEGER_BYTE: Byte = ByteString(":")(0)

  val replyTypeBytes = List(BULK_BYTE, ERROR_BYTE, STATUS_BYTE, MBULK_BYTE, INTEGER_BYTE)

  val R_BYTE = ByteString("\r")(0)
  val N_BYTE = ByteString("\n")(0)
}


