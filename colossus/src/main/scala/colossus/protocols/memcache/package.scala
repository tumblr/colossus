package colossus.protocols

import colossus.metrics.TagMap
import colossus.protocols.memcache.MemcacheClient.MemcacheClientLifter
import colossus.service.{ClientFactories, Protocol, ServiceClientFactory, TagDecorator}

package object memcache {

  trait Memcache extends Protocol {
    type Request  = MemcacheCommand
    type Response = MemcacheReply
  }

  object Memcache extends ClientFactories[Memcache, MemcacheClient](MemcacheClientLifter) {
    lazy val tagDecorator: TagDecorator[Memcache] = new TagDecorator[Memcache] {
      override def tagsFor(request: MemcacheCommand, response: MemcacheReply): TagMap = Map(
        "op" -> request.commandName.utf8String
      )
    }
    
    implicit lazy val clientFactory = ServiceClientFactory.basic("memcache", () => new MemcacheClientCodec, tagDecorator)

  }

}
