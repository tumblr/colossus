package colossus
package protocols

import service._

package object memcache {

  trait Memcache extends CodecDSL {
    type Input = MemcacheCommand
    type Output = MemcacheReply
  }

  implicit object MemcacheClientProvider extends ClientCodecProvider[Memcache] {
    def clientCodec = new MemcacheClientCodec
    def name = "memcache"
  }

}
