package colossus
package protocols

import akka.util.{ByteString, ByteStringBuilder}
import colossus.service._

import scala.language.higherKinds

package object redis {

  trait Redis extends Protocol {
    type Request = Command
    type Response = Reply
  }

  object RedisClientLifter extends ClientLifter[Redis, RedisClient] {
    override def lift[M[_]](client: Sender[Redis, M], clientConfig: Option[ClientConfig])(implicit async: Async[M]): RedisClient[M] = {
      new BasicLiftedClient(client, clientConfig) with RedisClient[M]
    }
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

