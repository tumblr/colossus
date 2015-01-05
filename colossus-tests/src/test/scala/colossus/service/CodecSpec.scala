package colossus
package service

import core.DataBuffer

import org.scalatest.{WordSpec, MustMatchers}

import akka.util.ByteString

class CodecSpec extends WordSpec with MustMatchers {

  "Codec" must {
    "decode all stops when databuffer is empty" in {
      val c = new Codec[String, String] {
        def decode(d: DataBuffer) = Some(DecodedResult.Static(ByteString(d.takeAll).utf8String))
        def encode(i: String) = DataBuffer(ByteString(i.toString))
        def reset(){}
      }
      val data = DataBuffer.fromByteString(ByteString("hello"))
      var build: List[String] = Nil
      c.decodeAll(data){
        case DecodedResult.Static(x) => build = x.value :: build
        case _ => throw new Exception("expecting non streamed result")
      }
      build must equal(List("hello"))
      build = Nil
      c.decodeAll(data){
        case DecodedResult.Static(x) => build = x.value :: build
        case _ => throw new Exception("expecting non streamed result")
      }
      build must equal(Nil)
    }
  }
}



