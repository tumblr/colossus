package colossus
package protocols.redis


import UnifiedProtocol._

import org.scalatest._

import akka.util.ByteString

class ScatterGatherSpec extends WordSpec with MustMatchers{
  val hasher = new CommandHasher[String] {
    def shardFor(key: ByteString) = key.utf8String(0).toString
  }

  "MGET scatter" must {
    val mget = Command(CMD_MGET, "a1", "b2" , "a2" , "b1", "c1")

    "scatter an MGET" in {
      val expected = Seq(
        "b" -> Command(CMD_MGET, "b2", "b1"),
        "a" -> Command(CMD_MGET, "a1", "a2"),
        "c" -> Command(CMD_MGET, "c1")
      )
      new MGetScatterGather(mget, hasher).scatter must equal(expected)

    }
    "gather replies" in {
      val s = new MGetScatterGather(mget, hasher)
      s.scatter
      val replies = Seq(
        MBulkReply(List(BulkReply(ByteString("vb2")), BulkReply(ByteString("vb1")))),
        MBulkReply(List(BulkReply(ByteString("va1")), BulkReply(ByteString("va2")))),
        MBulkReply(List(BulkReply(ByteString("vc1"))))
      )
      val expected = MBulkReply(mget.args.map{arg => BulkReply(ByteString("v" + arg.utf8String))})
      s.gather(replies) must equal(expected)
    }

  }

  "MSET scatter" must {
      val mset = Command(CMD_MSET, "b2", "vb2", "a1", "va1", "c1", "vc1", "b1", "vb1", "a2", "va2")
    "scatter an MSET" in {
      val expected = Seq(
        "b" -> Command(CMD_MSET, "b2", "vb2", "b1", "vb1"),
        "a" -> Command(CMD_MSET, "a1", "va1", "a2", "va2"),
        "c" -> Command(CMD_MSET, "c1", "vc1")
      )
      new MSetScatterGather(mset, hasher).scatter must equal(expected)


    }
    "gather replies" in {
      //notice - right now mset gathering doesn't depend on scattering (unlike mget which needs the indices), that may eventually change
      val replies = Seq(StatusReply("a"), StatusReply("b"), StatusReply("c"))
      val s = new MSetScatterGather(mset, hasher)
      s.gather(replies).isInstanceOf[StatusReply] must equal(true)

      val repliesWithError = Seq(StatusReply("a"), ErrorReply("b"), StatusReply("c"))
      s.gather(repliesWithError) must equal(ErrorReply("b"))
    }

  }

  "DEL scatter" must {
    //scattering is same as mget

    "gather replies" in {
      val replies = Seq(IntegerReply(0), IntegerReply(2), IntegerReply(1))
      new DelScatterGather(Command(CMD_DEL, List()), hasher).gather(replies) must equal(IntegerReply(3))
    }
    "convert to error" in {
      val repliesWithError = Seq(IntegerReply(0), ErrorReply("NOPE"), IntegerReply(1))
      new DelScatterGather(Command(CMD_DEL, List()), hasher).gather(repliesWithError) must equal(ErrorReply("NOPE"))
    }
  }

}
