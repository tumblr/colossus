package colossus
package protocols

import service._

import scala.language.higherKinds


package object memcache {

  trait Memcache extends Protocol {
    type Input = MemcacheCommand
    type Output = MemcacheReply
  }

  object Memcache extends ClientFactories[Memcache, MemcacheClient] {

    object defaults extends  {

      implicit val memcacheClientDefaults = new ClientCodecProvider[Memcache] {
        def clientCodec = new MemcacheClientCodec
        def name = "memcache"
      }


    }
  }

}

