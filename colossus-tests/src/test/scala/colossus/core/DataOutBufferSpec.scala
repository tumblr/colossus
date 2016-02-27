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

    "copy from bytebuffer" in {
      val data = ByteString("123456789")
      val n = new DynamicOutBuffer(5)
      n.write(DataBuffer(data))
      ByteString(n.data.takeAll) must equal(data)
    }
      
  }
}
