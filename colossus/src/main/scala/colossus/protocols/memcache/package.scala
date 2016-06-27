package colossus
package protocols

import service._

import scala.language.higherKinds


package object memcache {

  trait Memcache extends Protocol {
    type Request = MemcacheCommand
    type Response = MemcacheReply
  }

  object Memcache extends ClientFactories[Memcache, MemcacheClient] {

    implicit val clientFactory = ServiceClientFactory.staticClient("memcache", () => new MemcacheClientCodec)

  }

}

