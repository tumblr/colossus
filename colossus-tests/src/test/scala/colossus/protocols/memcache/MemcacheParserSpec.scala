package colossus


import org.scalatest._

import akka.util.ByteString

import core.DataBuffer
import protocols.memcache._

import parsing.ParseException
import parsing.DataSize._

class MemcacheParserSuite extends FlatSpec with ShouldMatchers{
  import MemcacheReply._

  "MemcacheParser" should "parse a value reply" in {
    val reply = DataBuffer(ByteString("VALUE foo 0 5\r\nhello\r\nEND\r\n"))
    val p = new MemcacheReplyParser
    p.parse(reply) should equal (Some(Value("foo", ByteString("hello"), 0)))
  }

  it should "parse actual reply from memcache" in {
    val reply = DataBuffer(ByteString(86, 65, 76, 85, 69, 32, 102, 111, 111, 32, 48, 32, 51, 13, 10, 104, 101, 108, 13, 10, 69, 78, 68, 13, 10))
    val p = new MemcacheReplyParser
    p.parse(reply) should equal (Some(Value("foo", ByteString("hel"), 0)))
  } 

  it should "parse a single END as no data" in {
    val reply = DataBuffer(ByteString("END\r\n"))
    val p = new MemcacheReplyParser
    p.parse(reply) should equal (Some(NoData))
  }

  it should "parse a blank error reply" in {
    val reply = DataBuffer(ByteString("ERROR\r\n"))
    val p = new MemcacheReplyParser
    p.parse(reply) should equal (Some(Error("ERROR")))
  }

  it should "accept a reply under the size limit" in {
    val reply = DataBuffer(ByteString("VALUE foo 0 5\r\nhello\r\nEND\r\n"))
    val p = MemcacheReplyParser(reply.size.bytes)
    p.parse(reply) should equal (Some(Value("foo", ByteString("hello"), 0)))
  }

  it should "reject a reply over the size limit" in {
    val reply = DataBuffer(ByteString("VALUE foo 0 5\r\nhello\r\nEND\r\n"))
    val p = MemcacheReplyParser((reply.size -1).bytes)
    intercept[ParseException] {
      p.parse(reply)
    }
  }
}
