package colossus

import core._
import testkit._

import akka.util.ByteString

class ConnectionSpec extends ColossusSpec {

  "WriteBuffer" must {
    "write some bytes" in {
      val m = new MockWriteBuffer(10)
      m.write(DataBuffer(ByteString("hello"))) must equal (WriteStatus.Complete)
      m.bytesAvailable must equal(5)
      m.readsEnabled must equal(true)
      m.writeReadyEnabled must equal(false)
      m.expectBufferNotCleared
      m.expectOneWrite(ByteString("hello"))
    }

    "partially buffer large data on overwrite" in {
      val m = new MockWriteBuffer(10)
      m.write(DataBuffer(ByteString("a1a2a3a4a5b1b2b3b4b5"))) must equal(WriteStatus.Partial)
      m.bytesAvailable must equal(0)
      m.readsEnabled must equal(true)
      m.writeReadyEnabled must equal(true)
      m.expectBufferNotCleared()
      m.expectOneWrite(ByteString("a1a2a3a4a5"))
      m.clearBuffer()
      m.readsEnabled must equal(true)
      m.writeReadyEnabled must equal(false)
      m.expectBufferCleared()
      m.expectOneWrite(ByteString("b1b2b3b4b5"))
    }

    "reject write calls when data is partially buffered" in {
      val m = new MockWriteBuffer(10)
      m.write(DataBuffer(ByteString("a1a2a3a4a5b1b2b3b4b5"))) must equal(WriteStatus.Partial)
      m.write(DataBuffer(ByteString("waaaatttt"))) must equal(WriteStatus.Zero)
    }
    

  }

}

