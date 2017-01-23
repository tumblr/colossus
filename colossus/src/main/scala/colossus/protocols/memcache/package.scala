package colossus
package protocols

import service._

package object memcache {

  trait Memcache extends Protocol {
    type Request = MemcacheCommand
    type Response = MemcacheReply
  }

  object Memcache extends ClientFactories[Memcache, MemcacheClient] {

    implicit lazy val clientFactory = ServiceClientFactory.basic("memcache", () => new MemcacheClientCodec)

    object defaults {
      //???
    }

  }

}

