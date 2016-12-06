package colossus
package testkit

import core.{DataBuffer, WriteStatus}
import akka.util.ByteString

import org.scalatest.{MustMatchers, WordSpec}

import scala.util.Try

class MockWriteBufferSpec extends WordSpec with MustMatchers{

  "MockWriteBuffer" must {

    "write" in {
      val m = new MockWriteBuffer(10)
      m.testWrite(DataBuffer(ByteString("abcd"))) must equal(WriteStatus.Complete)

      m.expectOneWrite(ByteString("abcd"))
    }

    "return partial when exceeds size" in {
      val m = new MockWriteBuffer(2)
      m.testWrite(DataBuffer(ByteString("abcd"))) must equal(WriteStatus.Partial)
      m.expectOneWrite(ByteString("ab"))
    }

    "return zero when full" in {
      val m = new MockWriteBuffer(2)
      m.testWrite(DataBuffer(ByteString("abcd"))) must equal(WriteStatus.Partial)
      m.testWrite(DataBuffer(ByteString("1234"))) must equal(WriteStatus.Zero)
      m.expectOneWrite(ByteString("ab"))
      m.expectNoWrite()
    }

    "clear buffer" in {
      val m = new MockWriteBuffer(2)
      m.testWrite(DataBuffer(ByteString("abcd"))) must equal(WriteStatus.Partial)
      m.expectOneWrite(ByteString("ab"))
      m.clearBuffer()
      m.continueWrite() must equal(true)
      m.testWrite(DataBuffer(ByteString("1234"))) must equal(WriteStatus.Partial)
      m.expectOneWrite(ByteString("cd"))
    }


  }
}


