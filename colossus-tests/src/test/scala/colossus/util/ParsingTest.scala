package colossus

import core.DataBuffer

import parsing._

import org.scalatest._

import akka.util.ByteString


class ParsingSuite extends WordSpec with MustMatchers{

  "UnsizedParseBuffer" must {
    "buffer some data" in {
      val b = new UnsizedParseBuffer(ByteString("!"))
      b.addData(DataBuffer(ByteString("hello"))) must equal(Incomplete)
      val buffer = DataBuffer(ByteString(" world!wat"))
      b.addData(buffer) must equal(Complete)
      b.result must equal(ByteString("hello world"))
      ByteString(buffer.takeAll) must equal(ByteString("wat"))
    }

    "ignore partial terminus" in {
      val b1 = DataBuffer(ByteString("hello12world123fail"))
      val p = new UnsizedParseBuffer(ByteString("123"))
      p.addData(b1) must equal(Complete)
      p.result must equal(ByteString("hello12world"))
      ByteString(b1.takeAll) must equal(ByteString("fail"))
    }

  }

  "SizedParseBuffer" must {
    "buffer some data" in {
      val b1 = ByteString("hello")
      val b2 = ByteString(" world!")
      val size = b1.size + b2.size
      val extra = ByteString("fail")

      val p = new SizedParseBuffer(size)
      p.addData(DataBuffer(b1)) must equal(Incomplete)
      val buf = DataBuffer(b2 ++ extra)
      p.addData(buf) must equal(Complete)
      p.result must equal(b1 ++ b2)
      ByteString(buf.takeAll) must equal(extra)
    }
  }

}

