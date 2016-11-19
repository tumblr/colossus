package colossus

import org.scalatest._

import akka.util.ByteString

import protocols.memcache.MemcacheCommand._
import colossus.protocols.memcache.NoCompressor


class MemcacheCommandSuite extends FlatSpec with ShouldMatchers{
  "MemcacheCommand" should "format a GET correctly" in {
    val experimental = Get(ByteString("test"))
    experimental.toString() should equal("get test\r\n")
    experimental.bytes(NoCompressor) should equal(ByteString("get test\r\n"))
  }

  it should "format a SET correctly" in {
    val experimental = Set(ByteString("key"), ByteString("value"), 30) // key, value, ttl
    experimental.toString() should equal("set key 0 30 5\r\nvalue\r\n")
    experimental.bytes(NoCompressor) should equal(ByteString("set key 0 30 5\r\nvalue\r\n"))
  }

  it should "format an ADD correctly" in {
    val experimental = Add(ByteString("key"), ByteString("magic"), 30)
    experimental.toString() should equal("add key 0 30 5\r\nmagic\r\n")
    experimental.bytes(NoCompressor) should equal(ByteString("add key 0 30 5\r\nmagic\r\n"))
  }

  it should "format a REPLACE correctly" in {
    val experimental = Replace(ByteString("key"), ByteString("magic"), 30)
    experimental.toString() should equal("replace key 0 30 5\r\nmagic\r\n")
    experimental.bytes(NoCompressor) should equal(ByteString("replace key 0 30 5\r\nmagic\r\n"))
  }

  it should "format an APPEND correctly" in {
    val experimental = Append(ByteString("key"), ByteString("magic"))
    experimental.toString() should equal("append key 0 0 5\r\nmagic\r\n")
    experimental.bytes(NoCompressor) should equal(ByteString("append key 0 0 5\r\nmagic\r\n"))
  }

  it should "format a PREPEND correctly" in {
    val experimental = Prepend(ByteString("key"), ByteString("magic"))
    experimental.toString() should equal("prepend key 0 0 5\r\nmagic\r\n")
    experimental.bytes(NoCompressor) should equal(ByteString("prepend key 0 0 5\r\nmagic\r\n"))
  }

/*  it should "format a CAS correctly" in {
    val experimental = Cas(ByteString("key"), "magic", 30)
    //experimental.bytes(NoCompressor) should equal(ByteString("cas key 0 30 5\r\nmagic\r\n"))
    experimental.toString() should equal("cas key 0 30 5\r\nmagic\r\n")
  }*/

  it should "format DELETE correctly" in {
    val experimental = Delete(ByteString("key"))
    experimental.toString() should equal("delete key\r\n")
    experimental.bytes(NoCompressor) should equal(ByteString("delete key\r\n"))
  }

  it should "format INCR correctly" in {
    val experimental = Incr(ByteString("key"), 1L)
    experimental.toString() should equal("incr key 1\r\n")
    experimental.bytes(NoCompressor) should equal(ByteString("incr key 1\r\n"))
  }

  it should "format DECR correctly" in {
    val experimental = Decr(ByteString("key"), 1L)
    experimental.toString() should equal("decr key 1\r\n")
    experimental.bytes(NoCompressor) should equal(ByteString("decr key 1\r\n"))
  }

  it should "format TOUCH correctly" in {
    val experimental = Touch(ByteString("key"), 30)
    experimental.toString() should equal("touch key 30\r\n")
    experimental.bytes(NoCompressor) should equal(ByteString("touch key 30\r\n"))
  }

}
