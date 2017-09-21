package colossus.protocols.redis

import org.scalatest._

import akka.util.ByteString

class ReplySpec extends WordSpec with MustMatchers {

  "Reply Encoding" must {
    //the double's on objects ensure we're returning a new DataBuffer every time

    "NilReply" in {
      NilReply.raw.takeAll must equal(ByteString("$-1\r\n"))
      NilReply.raw.takeAll must equal(ByteString("$-1\r\n"))
    }

    "EmptyMBulkReply" in {
      EmptyMBulkReply.raw.takeAll must equal(ByteString("*0\r\n"))
      EmptyMBulkReply.raw.takeAll must equal(ByteString("*0\r\n"))
    }

    "NilMBulkReply" in {
      NilMBulkReply.raw.takeAll must equal(ByteString("*-1\r\n"))
      NilMBulkReply.raw.takeAll must equal(ByteString("*-1\r\n"))
    }

  }
}
