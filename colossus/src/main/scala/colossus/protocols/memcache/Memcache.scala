package colossus
package protocols.memcache

import core._
import service._

import akka.util.{ByteString, ByteStringBuilder}
import java.util.zip.{Deflater, Inflater}

import parsing._
import DataSize._


object MemcacheCommand {
  val RN = ByteString("\r\n")

  case class Get(key: ByteString) extends MemcacheCommand {
    def bytes (compressor: Compressor)= ByteString("get ") ++ key ++ RN
  }
  object Get {
    def apply(key: String): Get = Get(ByteString(key))
  }
  case class Set(key: ByteString,  value: ByteString) extends MemcacheCommand {

    def bytes(compressor: Compressor) = {
      val b = new ByteStringBuilder
      val data = compressor.compress(value)
      b.append(ByteString("set ") )
      b.append(key )
      b.append(ByteString(s" 0 0 ${data.size}") )
      b.append(RN )
      b.append(data)
      b.append(RN)
      b.result
    }
  }
  object Set {
    def apply(key: String, value: String): Set = Set(ByteString(key), ByteString(value))
  }
  case class Delete(key: ByteString) extends MemcacheCommand {
    def bytes(c: Compressor) = ByteString("delete ") ++ key ++ RN
  }
  object Delete {
    def apply(key: String): Delete = Delete(ByteString(key))
  }
}
import MemcacheCommand.RN
//todo, ttl's flags, etc
sealed trait MemcacheCommand {
  //compressor should only be used on DATA
  def bytes(compressor: Compressor): ByteString
  override def toString = bytes(NoCompressor).utf8String
}

sealed trait MemcacheReply
sealed trait MemcacheHeader
object MemcacheReply {
  sealed trait DataReply extends MemcacheReply 
    
  case class Value(key: String, data: ByteString) extends DataReply
  case class Values(values: Seq[Value]) extends DataReply
  case object NoData extends DataReply

  //these are all one-line responses
  sealed trait MemcacheError extends MemcacheReply with MemcacheHeader {
    def error: String
  }
  case class Error(error: String) extends MemcacheError
  case class ClientError(error: String) extends MemcacheError
  case class ServerError(error: String) extends MemcacheError

  case object Stored extends MemcacheReply with MemcacheHeader
  case object NotFound  extends MemcacheReply with MemcacheHeader
  case object Deleted extends MemcacheReply with MemcacheHeader
  
}

class MemcacheReplyParser(maxSize: DataSize = MemcacheReplyParser.DefaultMaxSize) {

  private var parser = MemcacheReplyParser(maxSize)

  def parse(data: DataBuffer): Option[MemcacheReply] = parser.parse(data)

  def reset() {
    parser = MemcacheReplyParser.reply
  }
}

object MemcacheReplyParser {
  import parsing._
  import Combinators._
  import MemcacheReply._


  val DefaultMaxSize: DataSize = 1.MB

  def apply(size: DataSize = DefaultMaxSize) = maxSize(size, reply)

  def reply = delimitedString(' ', '\r') <~ byte |>{pieces => pieces.head match {
    case "VALUE"      => value(Nil, pieces(1), pieces(3).toInt)
    case "END"        => const(NoData)
    case "STORED"     => const(Stored)
    case "NOT_FOUND"  => const(NotFound)
    case "DELETED"    => const(Deleted)
    case "ERROR"      => const(Error("ERROR"))
    case other        => throw new ParseException(s"Unknown reply '$other'")
  }}
                                
  //returns either a Value or Values object depending if 1 or >1 values received
  def values(build: List[Value]): Parser[DataReply] = delimitedString(' ', '\r') <~ byte |>{pieces => pieces.head match {
    case "VALUE" => value(build, pieces(1), pieces(3).toInt)
    case "END" => const(if (build.size == 1) build.head else Values(build))
  }}

  //this parser starts immediately after the "VALUE" in the header, so it
  //parses the key and length in the header
  def value(build: List[Value], key: String, len: Int) = bytes(len) <~ bytes(2) |> {b => values(Value(key, b) :: build)}
}

case class MemcacheClientConfig(
  io: IOSystem,
  host: String,
  port: Int,
  compression: Boolean
)

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
    deflater.finish
    val builder = new ByteStringBuilder
    var numread = 0
    do {
      numread = deflater.deflate(buffer)
      builder.putBytes(buffer, 0, numread)
    } while (numread > 0)
    deflater.end
    builder.result
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
    inflater.end
    builder.result
  }

}



class MemcacheClientCodec(maxSize: DataSize = MemcacheReplyParser.DefaultMaxSize) extends Codec.ClientCodec[MemcacheCommand, MemcacheReply] {
  private var parser = new MemcacheReplyParser(maxSize)//(NoCompressor) //config

  def encode(cmd: MemcacheCommand): DataBuffer = DataBuffer(cmd.bytes(NoCompressor))
  def decode(data: DataBuffer): Option[MemcacheReply] = parser.parse(data)
  def reset(){
    parser = new MemcacheReplyParser(maxSize)//(NoCompressor)
  }
}

class MemcacheClient(config: ClientConfig, maxSize : DataSize = MemcacheReplyParser.DefaultMaxSize)
  extends ServiceClient[MemcacheCommand, MemcacheReply](
    codec   = new MemcacheClientCodec(maxSize),
    config  = config
  )

