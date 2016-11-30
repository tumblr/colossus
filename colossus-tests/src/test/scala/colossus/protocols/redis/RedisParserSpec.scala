package colossus

package protocols.redis

import core.DataBuffer

import org.scalatest._

import akka.util.ByteString

import scala.util.Success
import UnifiedProtocol._
import parsing._
import DataSize._

class FastCommandSuite extends FlatSpec with ShouldMatchers{

  //def commandParser = new SuperFastCommandParser
  def commandParser = RedisCommandParser.command

  "SuperFastCommandParser" should "parse a command" in {
    val command = ByteString("*3\r\n$3\r\nGET\r\n$4\r\nakey\r\n$3\r\nfoo\r\n")
    val expected = Some(Command(CMD_GET, Seq(ByteString("akey"), ByteString("foo"))))
    val parser = commandParser
    val actual = parser.parse(DataBuffer.fromByteString(command) )
    actual should equal (expected)
  }

  it should "parse a fragmented command" in {
    val command = ByteString("*3\r\n$3\r\nGET\r\n$4\r\nakey\r\n$3\r\nfoo\r\n")
    val expected = Some(Command(CMD_GET, Seq(ByteString("akey"), ByteString("foo"))))
    (1 until command.size).foreach{i =>
      val (p1, p2) = command.splitAt(i)
      val parser = commandParser
      parser.parse(DataBuffer.fromByteString(p1))
      val actual = parser.parse(DataBuffer.fromByteString(p2))
      actual should equal (expected)
    }
  }

  it should "parse newline in argument" in {
    val command = ByteString("*1\r\n$4\r\n\r\n\r\n\r\n")
    val expected = Some(Command("\r\n\r\n"))
    val parser = commandParser
    parser.parse(DataBuffer(command)) should equal (expected)
  }

  it should "parse inline command" in {
    val command = ByteString("SET FOO bar\r\n")
    val expected = Some(Command(CMD_SET, Seq(ByteString("FOO"), ByteString("bar"))))
    val parser = commandParser
    val actual = parser.parse(DataBuffer.fromByteString(command) )
    actual should equal (expected)
  }

  it should "parse two commands separately" in {
    val command1 = ByteString("*3\r\n$3\r\nGET\r\n$4\r\nakey\r\n$3\r\nfoo\r\n")
    val command2 = ByteString("*3\r\n$3\r\nGET\r\n$4\r\nbkey\r\n$3\r\nbar\r\n")
    val commands = DataBuffer.fromByteString(command1 ++ command2)
    val parser = commandParser
    parser.parse(commands) should equal (Some(Command("GET", "akey", "foo")))
    parser.parse(commands) should equal (Some(Command("GET", "bkey", "bar")))
  }

  it should "reject command with too few arguments" in {
    val command1 = ByteString("*4\r\n$3\r\nGET\r\n$4\r\nakey\r\n$3\r\nfoo\r\n")
    val command2 = ByteString("*3\r\n$3\r\nGET\r\n$4\r\nakey\r\n$3\r\nfoo\r\n")
    val parser = commandParser
    parser.parse(DataBuffer(command1)) should equal (None)
    intercept[ParseException]{
      parser.parse(DataBuffer(command2))
    }
  }

  it should "reject command with too-short argument" in {
    val command1 = ByteString("*3\r\n$3\r\nGET\r\n$4\r\nak\r\n$3\r\nfoo\r\n")
    val parser = commandParser
    intercept[ParseException]{
      parser.parse(DataBuffer(command1))
    }
  }

  it should "reject command with too-long argument" in {
    val command1 = ByteString("*3\r\n$3\r\nGET\r\n$4\r\nakOMGWTF\r\n$3\r\nfoo\r\n")
    val parser = commandParser
    intercept[ParseException]{
      parser.parse(DataBuffer(command1))
    }
  }

  it should "allow command under size limit" in {
    val command = ByteString("*1\r\n$3\r\nGET\r\n")
    val expected = Some(Command(CMD_GET))
    val parser = RedisCommandParser(command.size.bytes)
    val actual = parser.parse(DataBuffer.fromByteString(command) )
    actual should equal (expected)

  }

  it should "reject command over size limit" in {
    val command = ByteString("*1\r\n$3\r\nGET\r\n")
    val expected = Some(Command(CMD_GET))
    val parser = RedisCommandParser((command.size - 1).bytes)
    intercept[ParseException]{
      parser.parse(DataBuffer(command))
    }

  }

}

class FastReplySuite extends FlatSpec with ShouldMatchers{

  def replyParser = RedisReplyParser()

  "FastReplyParser" should "parse status reply" in {
    val reply = ByteString("+OK\r\n")
    val parser = replyParser
    val actual = parser.parse(DataBuffer.fromByteString(reply) )
    actual should equal (Some(StatusReply("OK")))
  }

  it should "parse bulk reply" in {
    val reply = ByteString("$5\r\nabcde\r\n")
    val parser = replyParser
    parser.parse(DataBuffer.fromByteString(reply)) should equal (Some(BulkReply(ByteString("abcde"))))
  }

  it should "parse nil reply" in {
    val reply = ByteString("$-1\r\n")
    val parser = replyParser
    parser.parse(DataBuffer.fromByteString(reply)) should equal (Some(NilReply))
  }

  it should "parse empty list reply" in {
    val reply = ByteString("*0\r\n")
    val parser = replyParser
    parser.parse(DataBuffer.fromByteString(reply)) should equal (Some(EmptyMBulkReply))
  }

  it should "parse nil mbulk reply" in {
    val reply = ByteString("*-1\r\n")
    val parser = replyParser
    parser.parse(DataBuffer.fromByteString(reply)) should equal (Some(NilMBulkReply))

  }

  it should "parse bulk reply in fragments" in {
    val reply = ByteString("$5\r\nabcde\r\n")
    (1 until reply.size-1).foreach{i =>
      val parser = replyParser
      val (p1, p2) = reply.splitAt(i)
      parser.parse(DataBuffer.fromByteString(p1)) should equal (None)
      parser.parse(DataBuffer.fromByteString(p2)) should equal (Some(BulkReply(ByteString("abcde"))))
    }
  }

  it should "parse mbulk reply" in {
    val reply = ByteString("*2\r\n$5\r\nabcde\r\n$3\r\n123\r\n")
    val parser = replyParser
    val expected = Some(MBulkReply(Seq(BulkReply(ByteString("abcde")), BulkReply(ByteString("123")))))
    parser.parse(DataBuffer.fromByteString(reply)) should equal (expected)
  }

  it should "parse mbulk in fragments" in {
    val reply = ByteString("*2\r\n$5\r\nabcde\r\n$3\r\n123\r\n")
    (1 until reply.size - 1).foreach{i =>
      val parser = replyParser
      val expected = Some(MBulkReply(Seq(BulkReply(ByteString("abcde")), BulkReply(ByteString("123")))))
      val (p1, p2) = reply.splitAt(i)
      parser.parse(DataBuffer.fromByteString(p1)) should equal (None)
      parser.parse(DataBuffer.fromByteString(p2)) should equal (expected)
    }
  }

  it should "parse large integer" in {
    val reply = IntegerReply(89840626838L)
    val parser = replyParser
    parser.parse(reply.raw) should equal(Some(reply))
  }
}

