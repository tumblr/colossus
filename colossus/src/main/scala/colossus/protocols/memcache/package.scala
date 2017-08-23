package colossus
package protocols

import colossus.protocols.memcache.MemcacheClient.MemcacheClientLifter
import colossus.service.{ClientFactories, Protocol, ServiceClientFactory}

package object memcache {

  trait Memcache extends Protocol {
    type Request  = MemcacheCommand
    type Response = MemcacheReply
  }

  object Memcache extends ClientFactories[Memcache, MemcacheClient](MemcacheClientLifter) {

    implicit lazy val clientFactory = ServiceClientFactory.basic("memcache", () => new MemcacheClientCodec)

  }

}
