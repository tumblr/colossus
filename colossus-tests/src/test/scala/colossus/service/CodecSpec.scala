package colossus.service

import org.scalatest.{MustMatchers, WordSpec}
import akka.util.ByteString
import colossus.controller.{Codec, Encoding}
import colossus.core.{DataBuffer, DataOutBuffer}

class CodecSpec extends WordSpec with MustMatchers {

  trait MyE extends Encoding {
    type Input  = String
    type Output = String
  }

  "Codec" must {
    "decode all stops when databuffer is empty" in {
      val c = new Codec[MyE] {
        def decode(d: DataBuffer) = Some(ByteString(d.takeAll).utf8String)
        def encode(i: String, buffer: DataOutBuffer) { buffer write DataBuffer(ByteString(i.toString)) }
        def reset() {}
        def endOfStream() = None
      }
      val data                = DataBuffer.fromByteString(ByteString("hello"))
      var build: List[String] = Nil
      c.decodeAll(data) { x =>
        build = x :: build
      }
      build must equal(List("hello"))
      build = Nil
      c.decodeAll(data) { x =>
        build = x :: build
      }
      build must equal(Nil)
    }
  }
}
