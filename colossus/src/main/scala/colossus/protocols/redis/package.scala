package colossus.protocols

import akka.util.{ByteString, ByteStringBuilder}
import colossus.protocols.redis.RedisClient.RedisClientLifter
import colossus.service.{ClientFactories, Protocol, ServiceClientFactory}

package object redis {

  trait Redis extends Protocol {
    type Request  = Command
    type Response = Reply
  }

  object Redis extends ClientFactories[Redis, RedisClient](RedisClientLifter) {

    implicit def clientFactory = ServiceClientFactory.basic("redis", () => new RedisClientCodec)

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
