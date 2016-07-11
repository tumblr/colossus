package colossus
package protocols.http

import core.{DataBlock, DataBuffer}
import testkit._
import stream._

class StreamHttpSpec extends ColossusSpec {


  "streamhttp codec" must {
    "decode a standard request into parts" in {
      val requestBytes = DataBuffer("GET /foo HTTP/1.1\r\ncontent-length: 10\r\nsomething-else: bleh\r\n\r\n0123456789")
      
      val codec = new StreamHttpServerCodec

      codec.decode(requestBytes) mustBe Some(RequestHead(HttpRequestHead(HttpMethod.Get, "/foo", HttpVersion.`1.1`, HttpHeaders.fromString(
        "content-length" -> "10", "something-else" -> "bleh"
      ))))
      codec.decode(requestBytes) mustBe Some(BodyData(DataBlock("0123456789")))
      codec.decode(requestBytes) mustBe Some(End)
      codec.decode(requestBytes) mustBe None
    }

    "decode a chunked request into chunks" in {
      val requestBytes = DataBuffer("GET /foo HTTP/1.1\r\ntransfer-encoding: chunked\r\nsomething-else: bleh\r\n\r\n5\r\nhello\r\n2\r\nok\r\n0\r\n\r\n")
      val codec = new StreamHttpServerCodec

      codec.decode(requestBytes) mustBe Some(RequestHead(HttpRequestHead(HttpMethod.Get, "/foo", HttpVersion.`1.1`, HttpHeaders.fromString(
        "transfer-encoding" -> "chunked", "something-else" -> "bleh"
      ))))

      codec.decode(requestBytes) mustBe Some(BodyData(DataBlock("hello")))
      codec.decode(requestBytes) mustBe Some(BodyData(DataBlock("ok")))
      codec.decode(requestBytes) mustBe Some(End)
      codec.decode(requestBytes) mustBe None
    }


  }

}
