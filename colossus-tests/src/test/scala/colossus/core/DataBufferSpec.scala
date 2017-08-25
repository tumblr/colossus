package colossus.core

import akka.util.{ByteString, ByteStringBuilder}
import colossus.testkit.ColossusSpec

class DataBufferSpec extends ColossusSpec {

  "DataBuffer" must {
    "read bytes one at a time" in {
      val b    = ByteString("Hello World!")
      val data = DataBuffer(b.asByteBuffer, b.size)
      data.size must equal(b.size)
      val builder = new ByteStringBuilder
      while (data.hasUnreadData) {
        builder.putByte(data.next)
      }
      data.size must equal(b.size)
      data.taken must equal(b.size)
      data.remaining must equal(0)
      builder.result must equal(b)
    }

    "read batches of bytes" in {
      val b       = ByteString("Hello World!")
      val data    = DataBuffer(b.asByteBuffer, b.size)
      val builder = new ByteStringBuilder
      builder.putBytes(data.take(3))
      data.remaining must equal(data.size - 3)
      builder.putBytes(data.take(234524))
      data.remaining must equal(0)
      builder.result must equal(b)
    }

    "read all remaining data" in {
      val b       = ByteString("Hello World!")
      val data    = DataBuffer(b.asByteBuffer, b.size)
      val builder = new ByteStringBuilder
      builder.putBytes(data.takeAll)
      data.remaining must equal(0)
      builder.result must equal(b)
    }

    "copy remaining data into new buffer" in {
      val b    = ByteString("Hello World!")
      val data = DataBuffer(b.asByteBuffer, b.size)
      ByteString(data.take(3)) must equal(ByteString("Hel"))
      val newBuffer = data.takeCopy
      data.hasUnreadData must equal(false)
      ByteString(newBuffer.takeAll) must equal(ByteString("lo World!"))
    }

  }

}
