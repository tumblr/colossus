package colossus
package protocols.memcache

import org.scalatest._

import akka.util.ByteString

class MemcacheKeySpec extends WordSpec with ShouldMatchers {
  "memcached key validator" should {
    "accept allowed keys" in {
      MemcacheCommand.validateKey(ByteString((33 to 255).map{_.toByte}.toArray)) // all the allowed values
      MemcacheCommand.validateKey(ByteString('0')) // smallest allowed key
      MemcacheCommand.validateKey(ByteString("0"*250)) // longest allowed key
    }
    "reject empty key" in {
      intercept[MemcacheEmptyKeyException] {
        MemcacheCommand.validateKey(ByteString())
      }
    }
    "reject key >250 chars" in {
      intercept[MemcacheKeyTooLongException] {
        MemcacheCommand.validateKey(ByteString("0"*251))
      }
    }
    "reject keys with space" in {
      val ex1 = intercept[MemcacheInvalidCharacterException] {
        MemcacheCommand.validateKey(ByteString("the rain in spain falls mainly on the plain"))
      }

      assert(ex1.position === 4)

      val ex2 = intercept[MemcacheInvalidCharacterException] {
        MemcacheCommand.validateKey(ByteString(" the"))
      }

      assert(ex2.position === 1)

      val ex3 = intercept[MemcacheInvalidCharacterException] {
        MemcacheCommand.validateKey(ByteString("the "))
      }

      assert(ex3.position === 4)
    }

    "accept keys with CR" in {
      MemcacheCommand.validateKey(ByteString("this\ris\rtotally\rok"))
    }

    "accept keys with LF" in {
      MemcacheCommand.validateKey(ByteString("a_great_deal_of_data\n"))
    }

    "accept keys with CR & LF" in {
      MemcacheCommand.validateKey(ByteString("\rfooled\nyou\r"))
    }

    "reject keys with CRLF" in {
      val ex1 = intercept[MemcacheInvalidCharacterException] {
        MemcacheCommand.validateKey(ByteString("Everything_seemed_fine\r\n"))
      }
      assert(ex1.position === 23)

      val ex2 = intercept[MemcacheInvalidCharacterException] {
        MemcacheCommand.validateKey(ByteString("\r\n"))
      }
      assert(ex2.position === 1)
    }
  }
}