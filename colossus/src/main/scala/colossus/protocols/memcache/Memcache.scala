package colossus
package protocols.memcache

import core._
import service._

import akka.util.{ByteString, ByteStringBuilder}
import java.util.zip.{Deflater, Inflater}

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
  val DECR    = ByteString("decr")
  val DELETE  = ByteString("delete")
  val GET     = ByteString("get")
  val INCR    = ByteString("incr")
  val PREPEND = ByteString("prepend")
  val REPLACE = ByteString("replace")
  val SET     = ByteString("set")
  val TOUCH   = ByteString("touch")

  val RN = ByteString("\r\n")
  val SP = ByteString(" ")

  val invalidKeyChars = (RN ++ SP).toSet[Byte]
}

//TODO: 'Flags' doesn't fully support the memcached protocol:  ie: using an Int doesn't allow us to utilize the full 32 unsigned int space
//TODO: implement CAS
//TODO: incrs/decrs are not utilizing full 64 bit unsigned int space since we are using singed Longs
object MemcacheCommand {

  import UnifiedProtocol._

  case class Get(keys: ByteString*) extends MemcacheCommand {

    val commandName = GET

    def bytes (compressor: Compressor = NoCompressor) = {
      val b = new ByteStringBuilder
      val totalKeyBytes = keys.foldLeft(0){ case(acc, key) =>
        acc + key.length + 1
      }
      //padding is 2..for the \r\n.  The spaces between keys are already accounted for in the foldLeft
      b.sizeHint(GET.size + totalKeyBytes + 2)
      b.append(GET)
      keys.foreach{x =>
        b.append(SP).append(x)
      }
      b.append(RN).result()
    }
  }

  case class Set(key: ByteString, value: ByteString, ttl: Int = 0, flags : Int = 0) extends MemcacheWriteCommand {
    val commandName = SET
  }

  case class Add(key: ByteString, value: ByteString, ttl: Int = 0, flags : Int = 0) extends MemcacheWriteCommand {
    val commandName = ADD
  }

  case class Replace(key: ByteString, value: ByteString, ttl: Int = 0, flags : Int = 0) extends MemcacheWriteCommand {
    val commandName = REPLACE
  }

  // Append does not take <flags> or <expiretime> but we have to provide them according to the protocol
  case class Append(key: ByteString, value: ByteString) extends MemcacheWriteCommand {
    val commandName = APPEND
    val ttl = 0
    val flags = 0
  }

  // Prepend does not take <flags> or <expiretime> but we have to provide them according to the protocol
  case class Prepend(key: ByteString, value: ByteString) extends MemcacheWriteCommand {
    val commandName = PREPEND
    val ttl = 0
    val flags = 0
  }

  /*object Cas {
    def apply(key: String, value: String, ttl: Integer = 0): Cas = Cas(ByteString(key), ByteString(value), ttl)
  }
  case class Cas(key: ByteString, value: ByteString, ttl: Integer) extends MemcachedCommand {
    def bytes(compressor: Compressor) = {
      val data = compressor.compress(value)
      formatCommand(ByteString("cas"), key, Some(data), Some(ByteString(s"${ttl}")), Some(ByteString(s"0")))
    }
  }*/

  case class Delete(key: ByteString) extends MemcacheCommand {

    val commandName = DELETE

    def bytes(c: Compressor = NoCompressor) = {
      val b = new ByteStringBuilder()
      //3 for SP and \R\N
      val hintSize = DELETE.size + key.size + 3
      b.sizeHint(hintSize)
      b.append(DELETE).append(SP).append(key).append(RN).result()
    }
  }

  sealed trait CounterCommand extends MemcacheCommand{
    def formatCommand(commandName : ByteString, key : ByteString, value : Long) : ByteString = {
      val b = new ByteStringBuilder
      val valStr = ByteString(value.toString)
      b.sizeHint(commandName.size + key.size + valStr.length + 4) //4 bytes one each for 2 spaces and an \r\n
      b.append(commandName).append(SP).append(key).append(SP).append(valStr).append(RN).result()
    }
  }

  case class Incr(key: ByteString, value: Long) extends CounterCommand {

    val commandName = INCR

    assert(value > 0, "Increment value must be non negative")
    def bytes(c: Compressor = NoCompressor) = formatCommand(INCR, key, value)
  }

  case class Decr(key: ByteString, value: Long) extends CounterCommand {

    val commandName = DECR

    assert(value > 0, "Decrement value must be non negative")
    def bytes(c: Compressor = NoCompressor) = formatCommand(DECR, key, value)
  }

  case class Touch(key: ByteString, ttl: Int) extends MemcacheCommand {

    val commandName = TOUCH

    assert(ttl > 0, "TTL Must be a non negative number")

    def bytes(c: Compressor = NoCompressor) = {
      val b = new ByteStringBuilder
      val ttlStr = ByteString(ttl.toString)
      b.sizeHint(TOUCH.size + key.size + ttlStr.length + 4) //4 one each for 2 spaces and an \r\n
      b.append(TOUCH).append(SP).append(key).append(SP).append(ttlStr).append(RN).result()
    }
  }
}

sealed class InvalidMemcacheKeyException(message: String, cause: Exception = null) extends MemcacheException(message, cause)

sealed class MemcacheEmptyKeyException extends
  InvalidMemcacheKeyException("Memcache keys must be at least 1 character.")

sealed class MemcacheKeyTooLongException(val key: ByteString) extends
  InvalidMemcacheKeyException(s"Memcache keys must be no longer than 250 characters. Provided key: ${key.utf8String}")

sealed class MemcacheInvalidCharacterException(val key: ByteString, val position: Int) extends
  InvalidMemcacheKeyException(s"Key contains invalid character at position $position. Provided key: ${key.utf8String}")

sealed trait MemcacheCommand {

  //compressor should only be used on DATA
  def bytes(compressor: Compressor): ByteString

  override def toString = bytes(NoCompressor).utf8String

  def commandName : ByteString
}

//set, add, replace, append, prepend
sealed trait MemcacheWriteCommand extends MemcacheCommand {

  import UnifiedProtocol._

  def key: ByteString
  def value: ByteString
  def ttl: Int
  def flags : Int

  def bytes(compressor : Compressor) : ByteString = {

    val b = new ByteStringBuilder


    /*write commands are of the format
    We don't support the [noreply] semantics at this time
    <COMMAND> <KEY> <FLAGS> <EXPTIME> <BYTECOUNT>\r\n<data>\r\n

    padding accounts for spaces, \r\n's, bytecount and exptime:
       4 spaces
    +  4 (2 \r\n's)
     ----
       8 */

    val padding = 8

    val flagsStr = ByteString(flags.toString)
    val ttlStr = ByteString(ttl.toString)
    val dataSizeStr = ByteString(value.size.toString)


    val sizeHint = commandName.length + flagsStr.length + ttlStr.length + dataSizeStr.length + value.size + padding

    b.sizeHint(sizeHint)
    b.append(commandName)
    b.append(SP)

    b.append(key)
    b.append(SP)

    b.append(flagsStr)
    b.append(SP)

    b.append(ttlStr)
    b.append(SP)

    b.append(dataSizeStr)
    b.append(RN)
    b.append(value)
    b.append(RN)

    b.result()

  }
}

object MemcacheException {
  def fromMemcacheError(error: MemcacheReply.MemcacheError) : MemcacheException = error match {
    case MemcacheReply.Error => new MemcacheErrorException
    case MemcacheReply.ClientError(message) => new MemcacheClientException(message)
    case MemcacheReply.ServerError(message) => new MemcacheServerException(message)
  }
}

class MemcacheException(message: String, cause: Exception = null) extends Exception(message, cause)
class MemcacheErrorException extends MemcacheException("Memcached returned an error. This likely due to a bad command string.")
class MemcacheClientException(message: String) extends MemcacheException(message)
class MemcacheServerException(message: String) extends MemcacheException(message)


sealed trait MemcacheReply
sealed trait MemcacheHeader
object MemcacheReply {
  sealed trait DataReply extends MemcacheReply

  case class Value(key: ByteString, data: ByteString, flags : Int) extends DataReply
  case class Counter(value : Long) extends DataReply
  case class Values(values: Vector[Value]) extends DataReply
  case object NoData extends DataReply

  //these are all one-line responses
  sealed trait MemcacheError extends MemcacheReply with MemcacheHeader

  case object Error extends MemcacheError
  case class ClientError(error: String) extends MemcacheError
  case class ServerError(error: String) extends MemcacheError

  case object Touched extends MemcacheReply with MemcacheHeader
  case object Stored extends MemcacheReply with MemcacheHeader
  case object NotFound  extends MemcacheReply with MemcacheHeader
  case object Deleted extends MemcacheReply with MemcacheHeader
  case object NotStored extends MemcacheReply with MemcacheHeader
  case object Exists extends MemcacheReply with MemcacheHeader

}

class MemcacheReplyParser() {

  private var parser = MemcacheReplyParser()

  def parse(data: DataBuffer): Option[MemcacheReply] = parser.parse(data)

  def reset() {
    parser = MemcacheReplyParser.reply
  }
}

object MemcacheReplyParser {
  import parsing._
  import Combinators._
  import MemcacheReply._

  def apply() = reply

  def reply = delimitedString(' ', '\r') <~ byte |>{pieces => pieces.head match {
    case "VALUE"          => value(Vector.empty, pieces(1), pieces(2).toInt, pieces(3).toInt)
    case "END"            => const(NoData)
    case "NOT_STORED"     => const(NotStored)
    case "STORED"         => const(Stored)
    case "EXISTS"         => const(Exists)
    case "NOT_FOUND"      => const(NotFound)
    case "DELETED"        => const(Deleted)
    case "TOUCHED"        => const(Touched)
    case "CLIENT_ERROR"   => const(ClientError(pieces.tail.mkString(" ")))
    case "SERVER_ERROR"   => const(ServerError(pieces.tail.mkString(" ")))
    case "ERROR"          => const(Error)
    case other if isNumeric(other) => const(Counter(other.toLong))
    case other        => throw new ParseException(s"Unknown reply '$other'")
  }}

  def isNumeric(str : String) = str.forall(_.isDigit)

  //returns either a Value or Values object depending if 1 or >1 values received
  def values(build: Vector[Value]): Parser[DataReply] = delimitedString(' ', '\r') <~ byte |>{pieces => pieces.head match {
    case "VALUE" => value(build, pieces(1), pieces(2).toInt, pieces(3).toInt)
    case "END" => const(if (build.size == 1) build.head else Values(build))
  }}

  def value(build: Vector[Value], key: String, flags : Int, len: Int) = bytes(len) <~ bytes(2) |> {b => values(build :+ Value(ByteString(key), ByteString(b), flags))}
}

trait Compressor {
  def compress(bytes: ByteString): ByteString
  def decompress(bytes: ByteString): ByteString
}

object NoCompressor extends Compressor{
  def compress(bytes: ByteString): ByteString = bytes
  def decompress(bytes: ByteString): ByteString = bytes
}

class ZCompressor(bufferKB: Int = 10) extends Compressor {
  val buffer = new Array[Byte](1024 * bufferKB)

  def compress(bytes: ByteString): ByteString = {
    val deflater = new Deflater
    deflater.setInput(bytes.toArray)
    deflater.finish()
    val builder = new ByteStringBuilder
    var numread = 0
    do {
      numread = deflater.deflate(buffer)
      builder.putBytes(buffer, 0, numread)
    } while (numread > 0)
    deflater.end()
    builder.result()
  }

  def decompress(bytes: ByteString): ByteString = {
    val inflater = new Inflater
    inflater.setInput(bytes.toArray)
    val builder = new ByteStringBuilder
    var numread = 0
    do {
      numread = inflater.inflate(buffer)
      builder.putBytes(buffer, 0, numread)
    } while (numread > 0)
    inflater.end()
    builder.result()
  }

}

class MemcacheClientCodec() extends Codec.ClientCodec[MemcacheCommand, MemcacheReply] {
  private var parser = new MemcacheReplyParser()//(NoCompressor) //config

  def encode(cmd: MemcacheCommand): DataReader = DataBuffer(cmd.bytes(NoCompressor))
  def decode(data: DataBuffer): Option[DecodedResult[MemcacheReply]] = DecodedResult.static(parser.parse(data))
  def reset(){
    parser = new MemcacheReplyParser()//(NoCompressor)
  }
}
