package colossus.protocols

import akka.util.{ByteString, ByteStringBuilder}
import colossus.metrics.TagMap
import colossus.protocols.redis.RedisClient.RedisClientLifter
import colossus.service.{ClientFactories, Protocol, ServiceClientFactory, TagDecorator}

package object redis {

  trait Redis extends Protocol {
    type Request  = Command
    type Response = Reply
  }

  object Redis extends ClientFactories[Redis, RedisClient](RedisClientLifter) {
    lazy val tagDecorator: TagDecorator[Redis] = new TagDecorator[Redis] {
      override def tagsFor(request: Command, response: Reply): TagMap = Map(
        "op" -> request.command
      )
    }
    
    implicit def clientFactory = 
      ServiceClientFactory.basic("redis", () => new RedisClientCodec, tagDecorator)

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
