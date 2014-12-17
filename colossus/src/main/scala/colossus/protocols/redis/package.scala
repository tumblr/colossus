package colossus
package protocols

import colossus.parsing.DataSize
import core._
import service._

import akka.util.{ByteString, ByteStringBuilder}
import scala.util.{Success, Failure}
import Codec._

package object redis {

  trait Redis extends CodecDSL {
    type Input = Command
    type Output = Reply
  }

  implicit object RedisCodecProvider extends CodecProvider[Redis] {
    def provideCodec() = new RedisServerCodec

    def errorResponse(request: Command, reason: Throwable) = ErrorReply(s"Error (${reason.getClass.getName}): ${reason.getMessage}")
  }

  implicit object RedisClientCodecProvider extends ClientCodecProvider[Redis] {
    def clientCodec() = new RedisClientCodec
    val name = "redis"
  }

  class RedisClient(config: ClientConfig, maxSize : DataSize = RedisReplyParser.DefaultMaxSize) extends ServiceClient(
    codec     = new RedisClientCodec(maxSize),
    config    = config
  ) {
    import UnifiedProtocol._
    def info: Callback[Map[String, String]] = send(Command(CMD_INFO)).mapTry{_.flatMap{
      case BulkReply(data) => Success(InfoParser(data.utf8String))
      case other => Failure(new Error("Invalid response type"))
    }}
  }

  object InfoParser {
    def apply(infoString: String) = infoString.split("\n")
      .collect{case s if(s.contains(":")) => s.split(":")}
      .map{case items => (items(0), items(1).trim)}
      .toMap    

  }


  implicit object RedisServerCodecFactory extends ServerCodecFactory[Command, Reply] {
    def apply() = new RedisServerCodec
  }
  implicit object RedisClientCodecFactory extends ClientCodecFactory[Command, Reply] {
    def apply() = new RedisClientCodec
  }


  object UnifiedBuilder {

    import UnifiedProtocol._

    def buildArg(data: ByteString, builder: ByteStringBuilder) {
      builder append ARG_LEN
      builder append ByteString(data.size.toString)
      builder append RN
      builder append data
      builder append RN
    }

  }
}

