package colossus
package protocols.memcache

import org.scalatest._

import akka.util.ByteString

import core.DataBuffer

class MemcacheKeySpec extends WordSpec with ShouldMatchers {
  "memcached key validator" should {
    "accept allowed keys" in {
      MemcachedKey.validateKey(ByteString((33 to 255).map{_.toByte}.toArray)) // all the allowed values
      MemcachedKey.validateKey(ByteString('0')) // smallest allowed key
      MemcachedKey.validateKey(ByteString("0"*250)) // longest allowed key
    }
    "reject empty key" in {
      intercept[InvalidMemcacheKeyException] {
        MemcachedKey.validateKey(ByteString())
      }
    }
    "reject key >250 chars" in {
      intercept[InvalidMemcacheKeyException] {
        MemcachedKey.validateKey(ByteString("0"*251))
      }
    }
    "reject keys with bytes <= 0x20" in {
      intercept[InvalidMemcacheKeyException] {
        MemcachedKey.validateKey(ByteString(' '))
      }

      intercept[InvalidMemcacheKeyException] {
        MemcachedKey.validateKey(ByteString(0x00))
      }

      intercept[InvalidMemcacheKeyException] {
        MemcachedKey.validateKey(ByteString(0x09))
      }

      intercept[InvalidMemcacheKeyException] {
        MemcachedKey.validateKey(ByteString(0x0A))
      }
    }
  }
}