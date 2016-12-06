package colossus
package protocols.memcache


import org.scalatest._

import akka.util.ByteString

import core.DataBuffer

import colossus.parsing.{DataSize, ParseException}

class MemcacheParserSpec extends FlatSpec with ShouldMatchers{
  import MemcacheReply._

  "MemcacheParser" should "parse a value reply" in {
    val reply = DataBuffer(ByteString("VALUE foo 0 5\r\nhello\r\nEND\r\n"))
    val p = new MemcacheReplyParser
    p.parse(reply) should equal (Some(Value(ByteString("foo"), ByteString("hello"), 0)))
  }

  it should "parse actual reply from memcache" in {
    val reply = DataBuffer(ByteString(86, 65, 76, 85, 69, 32, 102, 111, 111, 32, 48, 32, 51, 13, 10, 104, 101, 108, 13, 10, 69, 78, 68, 13, 10))
    val p = new MemcacheReplyParser
    p.parse(reply) should equal (Some(Value(ByteString("foo"), ByteString("hel"), 0)))
  }

  it should "parse a single END as no data" in {
    val reply = DataBuffer(ByteString("END\r\n"))
    val p = new MemcacheReplyParser
    p.parse(reply) should equal (Some(NoData))
  }

  it should "parse a server error" in {
    val reply = DataBuffer(ByteString("SERVER_ERROR out of memory storing object\r\n"))
    val p = new MemcacheReplyParser
    p.parse(reply) should equal (Some(ServerError("out of memory storing object")))
  }

  it should "parse a blank error reply" in {
    val reply = DataBuffer(ByteString("ERROR\r\n"))
    val p = new MemcacheReplyParser
    p.parse(reply) should equal (Some(Error))
  }
}
