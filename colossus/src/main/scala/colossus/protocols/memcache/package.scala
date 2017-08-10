package colossus
package protocols

import service._

import scala.language.higherKinds

package object memcache {

  trait Memcache extends Protocol {
    type Request = MemcacheCommand
    type Response = MemcacheReply
  }

  object MemcacheClientLifter extends ClientLifter[Memcache, MemcacheClient] {
    override def lift[M[_]](client: Sender[Memcache, M], clientConfig: Option[ClientConfig])(implicit async: Async[M]): MemcacheClient[M] = {
      new BasicLiftedClient(client, clientConfig) with MemcacheClient[M]
    }
  }

  object Memcache extends ClientFactories[Memcache, MemcacheClient](MemcacheClientLifter) {

    implicit lazy val clientFactory = ServiceClientFactory.basic("memcache", () => new MemcacheClientCodec)

  }

}

