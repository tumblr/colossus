package colossus.protocols.memcache

import colossus.controller.Codec
import colossus.core._
import akka.util.{ByteString, ByteStringBuilder}

/*
 * Memcache protocol for Colossus, implements a majority of the commands with the exception of some of the
 * administrative commands like stats. It could be easily added though.
 *
 * It's important to note that the memcache keys cannot be over 250 bytes and will be rejected
 *
 * We also make sure that the keys are "well formed", removing all control characters and spaces, which
 * are not allowed by Memcached
 *
 * I've grabbed a snippet from memcached docs about ttls found here: https://github.com/memcached/memcached/blob/master/doc/protocol.txt
 *
 * TTLs can be a unix timestamp for an integer that is no more than 30 days in seconds (2592000 seconds). If the TTL is greater
 * than 30 days in seconds, the server will consider it to be real Unix time value rather than an offset from current time.
 *
 */

object UnifiedProtocol {
  val ADD     = ByteString("add")
  val APPEND  = ByteString("append")
  val CAS     = ByteString("cas")
  val DECR    = ByteString("decr")
  val DELETE  = ByteString("delete")
  val GET     = ByteString("get")
  val GETS    = ByteString("gets")
  val INCR    = ByteString("incr")
  val PREPEND = ByteString("prepend")
  val REPLACE = ByteString("replace")
  val SET     = ByteString("set")
  val TOUCH   = ByteString("touch")

  val EndOfMessageDelimiter     = ByteString("\r\n")
  val MemcachedMessageDelimiter = ByteString(" ")

  val invalidKeyChars: Set[Byte] = (EndOfMessageDelimiter ++ MemcachedMessageDelimiter).toSet[Byte]
}

//TODO: 'Flags' doesn't fully support the memcached protocol:  ie: using an Int doesn't allow us to utilize the full 32 unsigned int space
//TODO: incrs/decrs are not utilizing full 64 bit unsigned int space since we are using signed Longs
object MemcacheCommand {

  import UnifiedProtocol._

  private def getMemcacheCommandMsg(commandName: ByteString)(keys: ByteString*) = {
    val buffer = new ByteStringBuilder
    val totalKeyBytes = keys.foldLeft(0) {
      case (acc, key) =>
        acc + key.length + 1
    }

    buffer.sizeHint(commandName.size + totalKeyBytes + EndOfMessageDelimiter.size)
    buffer.append(commandName)
    keys.foreach { x =>
      buffer.append(MemcachedMessageDelimiter).append(x)
    }
    buffer.append(EndOfMessageDelimiter).result()
  }

  case class Get(keys: ByteString*) extends MemcacheCommand {
    val commandName: ByteString = GET

    def memcacheCommandMsg(): ByteString = {
      getMemcacheCommandMsg(commandName)(keys: _*)
    }
  }

  case class Gets(keys: ByteString*) extends MemcacheCommand {
    val commandName: ByteString = GETS

    def memcacheCommandMsg(): ByteString = {
      getMemcacheCommandMsg(commandName)(keys: _*)
    }
  }

  case class Set(key: ByteString, value: ByteString, ttl: Int = 0, flags: Int = 0) extends MemcacheWriteCommand {
    val commandName: ByteString = SET
    override def casUniqueMaybe: Option[Int] = None
  }

  case class Cas(key: ByteString,
               value: ByteString,
                    ttl: Int = 0,
                  flags: Int = 0,
      casUniqueMaybe: Option[Int]) extends MemcacheWriteCommand {
    val commandName: ByteString = CAS
  }

  case class Add(key: ByteString, value: ByteString, ttl: Int = 0, flags: Int = 0) extends MemcacheWriteCommand {
    val commandName: ByteString = ADD
    val casUniqueMaybe: Option[Int] = None
  }

  case class Replace(key: ByteString, value: ByteString, ttl: Int = 0, flags: Int = 0) extends MemcacheWriteCommand {
    val commandName: ByteString = REPLACE
    val casUniqueMaybe: Option[Int] = None
  }

  // Append does not take <flags> or <expiretime> but we have to provide them according to the protocol
  case class Append(key: ByteString, value: ByteString) extends MemcacheWriteCommand {
    val commandName: ByteString = APPEND
    val ttl         = 0
    val flags       = 0
    val casUniqueMaybe: Option[Int] = None
  }

  // Prepend does not take <flags> or <expiretime> but we have to provide them according to the protocol
  case class Prepend(key: ByteString, value: ByteString) extends MemcacheWriteCommand {
    val commandName: ByteString = PREPEND
    val ttl         = 0
    val flags       = 0
    val casUniqueMaybe: Option[Int] = None
  }

  case class Delete(key: ByteString) extends MemcacheCommand {
    val commandName: ByteString = DELETE

    def memcacheCommandMsg(): ByteString = {
      val b = new ByteStringBuilder()
      //3 for SP and \R\N
      val hintSize = DELETE.size + key.size + 3
      b.sizeHint(hintSize)
      b.append(DELETE).append(MemcachedMessageDelimiter).append(key).append(EndOfMessageDelimiter).result()
    }
  }

  sealed trait CounterCommand extends MemcacheCommand {
    def formatCommand(commandName: ByteString, key: ByteString, value: Long): ByteString = {
      val b      = new ByteStringBuilder
      val valStr = ByteString(value.toString)
      b.sizeHint(commandName.size + key.size + valStr.length + 4) //4 bytes one each for 2 spaces and an \r\n
      b.append(commandName).append(MemcachedMessageDelimiter).append(key).append(MemcachedMessageDelimiter).append(valStr).append(EndOfMessageDelimiter).result()
    }
  }

  case class Incr(key: ByteString, value: Long) extends CounterCommand {

    val commandName = INCR

    assert(value > 0, "Increment value must be non negative")
    def memcacheCommandMsg(): ByteString = formatCommand(INCR, key, value)
  }

  case class Decr(key: ByteString, value: Long) extends CounterCommand {

    val commandName = DECR

    assert(value > 0, "Decrement value must be non negative")
    def memcacheCommandMsg(): ByteString = formatCommand(DECR, key, value)
  }

  case class Touch(key: ByteString, ttl: Int) extends MemcacheCommand {

    val commandName = TOUCH

    assert(ttl > 0, "TTL Must be a non negative number")

    def memcacheCommandMsg(): ByteString = {
      val builder      = new ByteStringBuilder
      val ttlStr = ByteString(ttl.toString)
      builder.sizeHint(TOUCH.size + key.size + ttlStr.length + 4) //4 one each for 2 spaces and an \r\n
      builder.append(TOUCH)
        .append(MemcachedMessageDelimiter)
        .append(key)
        .append(MemcachedMessageDelimiter)
        .append(ttlStr)
        .append(EndOfMessageDelimiter)
        .result()
    }
  }
}

sealed class InvalidMemcacheKeyException(message: String, cause: Exception = null)
    extends MemcacheException(message, cause)

sealed class MemcacheEmptyKeyException
    extends InvalidMemcacheKeyException("Memcache keys must be at least 1 character.")

sealed class MemcacheKeyTooLongException(val key: ByteString)
    extends InvalidMemcacheKeyException(
      s"Memcache keys must be no longer than 250 characters. Provided key: ${key.utf8String}")

sealed class MemcacheInvalidCharacterException(val key: ByteString, val position: Int)
    extends InvalidMemcacheKeyException(
      s"Key contains invalid character at position $position. Provided key: ${key.utf8String}")

sealed trait MemcacheCommand {

  def memcacheCommandMsg(): ByteString

  override def toString = memcacheCommandMsg().utf8String

  def commandName: ByteString
}

//set, cas, add, replace, append, prepend
sealed trait MemcacheWriteCommand extends MemcacheCommand {

  import UnifiedProtocol._

  def key: ByteString
  def value: ByteString
  def ttl: Int
  def flags: Int
  def casUniqueMaybe: Option[Int]

  /**
    * Write commands are of the format
    * <COMMAND> <KEY> <FLAGS> <EXPTIME> <BYTECOUNT> [<CAS UNIQUE>]\r\n<data>\r\n
    * We don't support the [noreply] semantics at this time
    * @return ByteString of a memcache write command
    */
  def memcacheCommandMsg(): ByteString = {

    val buffer = new ByteStringBuilder

    val flagsStr     = ByteString(flags.toString)
    val ttlStr       = ByteString(ttl.toString)
    val dataSizeStr  = ByteString(value.size.toString)
    val casUniqueStr = ByteString(casUniqueMaybe.getOrElse("").toString)

    val padding = getMemcacheCommandPadding(casUniqueStr.nonEmpty)

    val sizeHint = commandName.length +
                      flagsStr.length +
                        ttlStr.length +
                   dataSizeStr.length +
                  casUniqueStr.length +
                           value.size +
                              padding

    buffer.sizeHint(sizeHint)
    buffer.append(commandName)
    buffer.append(MemcachedMessageDelimiter)

    buffer.append(key)
    buffer.append(MemcachedMessageDelimiter)

    buffer.append(flagsStr)
    buffer.append(MemcachedMessageDelimiter)

    buffer.append(ttlStr)
    buffer.append(MemcachedMessageDelimiter)

    buffer.append(dataSizeStr)

    if (casUniqueStr.nonEmpty) {
      buffer.append(MemcachedMessageDelimiter)
      buffer.append(casUniqueStr)
    }

    buffer.append(EndOfMessageDelimiter)
    buffer.append(value)
    buffer.append(EndOfMessageDelimiter)

    buffer.result()
  }

  /**
    * padding accounts for spaces, \r\n's, bytecount and exptime:
    *         4 or 5 spaces, depending if <CAS UNIQUE> is present
    *      +  4 (2 \r\n's)
    *       ----
    * @return 8 or 9, depending if <CAS UNIQUE> is present
    */
  private def getMemcacheCommandPadding(casUniquePresent: Boolean): Int = 8 + (if (casUniquePresent) 1 else 0)
}

object MemcacheException {
  def fromMemcacheError(error: MemcacheReply.MemcacheError): MemcacheException = error match {
    case MemcacheReply.Error                => new MemcacheErrorException
    case MemcacheReply.ClientError(message) => new MemcacheClientException(message)
    case MemcacheReply.ServerError(message) => new MemcacheServerException(message)
  }
}

class MemcacheException(message: String, cause: Exception = null) extends Exception(message, cause)
class MemcacheErrorException
    extends MemcacheException("Memcached returned an error. This likely due to a bad command string.")
class MemcacheClientException(message: String) extends MemcacheException(message)
class MemcacheServerException(message: String) extends MemcacheException(message)

sealed trait MemcacheReply
sealed trait MemcacheHeader
object MemcacheReply {
  sealed trait DataReply extends MemcacheReply

  case class Value(key: ByteString, data: ByteString, flags: Int)                    extends DataReply
  case class CasValue(key: ByteString, data: ByteString, flags: Int, casUnique: Int) extends DataReply
  case class Counter(value: Long)                                                    extends DataReply
  case class Values(values: Vector[Value])                                           extends DataReply
  case object NoData                                                                 extends DataReply

  //these are all one-line responses
  sealed trait MemcacheError extends MemcacheReply with MemcacheHeader

  case object Error                     extends MemcacheError
  case class ClientError(error: String) extends MemcacheError
  case class ServerError(error: String) extends MemcacheError

  case object Touched   extends MemcacheReply with MemcacheHeader
  case object Stored    extends MemcacheReply with MemcacheHeader
  case object NotFound  extends MemcacheReply with MemcacheHeader
  case object Deleted   extends MemcacheReply with MemcacheHeader
  case object NotStored extends MemcacheReply with MemcacheHeader
  case object Exists    extends MemcacheReply with MemcacheHeader

}

class MemcacheReplyParser() {

  private var parser = MemcacheReplyParser()

  def parse(data: DataBuffer): Option[MemcacheReply] = parser.parse(data)

  def reset() {
    parser = MemcacheReplyParser.reply
  }
}

object MemcacheReplyParser {
  import colossus.util._
  import colossus.util.Combinators._
  import MemcacheReply._

  val NumberOfPiecesInCasValueReply = 5

  def apply() = reply

  def reply = delimitedString(' ', '\r') <~ byte |> { pieces: Seq[String] =>
    pieces.head match {
      case "VALUE"                   =>
        if (pieces.size < NumberOfPiecesInCasValueReply) {
          value(Vector.empty, pieces(1), pieces(2).toInt, pieces(3).toInt)
        } else {
          casValue(pieces(1), pieces(2).toInt, pieces(3).toInt, pieces(4).toInt)
        }
      case "END"                     => const(NoData)
      case "NOT_STORED"              => const(NotStored)
      case "STORED"                  => const(Stored)
      case "EXISTS"                  => const(Exists)
      case "NOT_FOUND"               => const(NotFound)
      case "DELETED"                 => const(Deleted)
      case "TOUCHED"                 => const(Touched)
      case "CLIENT_ERROR"            => const(ClientError(pieces.tail.mkString(" ")))
      case "SERVER_ERROR"            => const(ServerError(pieces.tail.mkString(" ")))
      case "ERROR"                   => const(Error)
      case other if isNumeric(other) => const(Counter(other.toLong))
      case other                     => throw new ParseException(s"Unknown reply '$other'")
    }
  }

  def isNumeric(str: String): Boolean = str.forall(_.isDigit)

  //returns either a Value or Values object depending if 1 or >1 values received
  def values(build: Vector[Value]): Parser[DataReply] = delimitedString(' ', '\r') <~ byte |> { pieces =>
    pieces.head match {
      case "VALUE" => value(build, pieces(1), pieces(2).toInt, pieces(3).toInt)
      case "END"   => const(if (build.size == 1) build.head else Values(build))
    }
  }

  def value(build: Vector[Value], key: String, flags: Int, len: Int): Parser[DataReply] =
    bytes(len) <~ bytes(2) |> { b => values(build :+ Value(ByteString(key), ByteString(b), flags))
  }

  def casValue(key: String, flags: Int, len: Int, casUnique: Int): Parser[CasValue] =
    bytes(len) <~ bytes(2) |> { b => const(CasValue(ByteString(key), ByteString(b), flags, casUnique))
  }
}

class MemcacheClientCodec() extends Codec.Client[Memcache] {
  private var parser = new MemcacheReplyParser()

  def encode(cmd: MemcacheCommand, buffer: DataOutBuffer) {
    buffer.write(cmd.memcacheCommandMsg())
  }
  def decode(data: DataBuffer): Option[MemcacheReply] = parser.parse(data)
  def reset() {
    parser = new MemcacheReplyParser()
  }
  def endOfStream() = None
}
