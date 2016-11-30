package colossus
package core

import testkit._
import akka.util.ByteString

class DataOutBufferSpec extends ColossusSpec {

  "DynamicOutBuffer" must {

    "handle overflow" in {
      val data = ByteString("123456789")
      val n = new DynamicOutBuffer(5)
      n.write(data)
      ByteString(n.data.takeAll) must equal(data)
    }

    "resize overflow" in {
      val d1 = ByteString("123")
      val d2 = ByteString("456")
      val n = new DynamicOutBuffer(2)
      n.write(d1)
      n.write(d2)
      ByteString(n.data.takeAll) must equal(d1 ++ d2)
    }

    "copy from bytebuffer" in {
      val data = ByteString("123456789")
      val n = new DynamicOutBuffer(5)
      n.write(DataBuffer(data))
      ByteString(n.data.takeAll) must equal(data)
    }

  }
}
