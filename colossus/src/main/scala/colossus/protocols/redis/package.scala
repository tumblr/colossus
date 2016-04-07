package colossus
package protocols

import akka.util.{ByteString, ByteStringBuilder}
import colossus.service._


package object redis {

  trait Redis extends Protocol {
    type Input = Command
    type Output = Reply
  }

  object Redis extends ClientFactories[Redis, RedisClient]{
    object defaults {

      implicit val redisServerDefaults = new ServiceCodecProvider[Redis] {
        def provideCodec() = new RedisServerCodec
        def errorResponse(error: ProcessingFailure[Command]) = ErrorReply(s"Error (${error.reason.getClass.getName}): ${error.reason.getMessage}")
      }

      implicit val redisClientDefaults = new ClientCodecProvider[Redis] {
        def clientCodec() = new RedisClientCodec
        val name = "redis"
      }
    }

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

