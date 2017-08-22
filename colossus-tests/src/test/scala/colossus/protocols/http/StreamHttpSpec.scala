package colossus.protocols.http

import colossus.core._
import colossus.testkit.ColossusSpec
import streaming._

import org.scalamock.scalatest.MockFactory

class StreamHttpSpec extends ColossusSpec with MockFactory {

  "streamhttp codec" must {
    "decode a standard request into parts" in {
      val requestBytes =
        DataBuffer("GET /foo HTTP/1.1\r\ncontent-length: 10\r\nsomething-else: bleh\r\n\r\n0123456789")

      val codec = new StreamHttpServerCodec

      codec.decode(requestBytes) mustBe Some(
        Head(
          HttpRequestHead(HttpMethod.Get,
                          "/foo",
                          HttpVersion.`1.1`,
                          HttpHeaders.fromString(
                            "content-length" -> "10",
                            "something-else" -> "bleh"
                          ))))
      codec.decode(requestBytes) mustBe Some(Data(DataBlock("0123456789")))
      codec.decode(requestBytes) mustBe Some(End)
      codec.decode(requestBytes) mustBe None
    }

    "decode a chunked request into chunks" in {
      val requestBytes1 =
        DataBuffer("GET /foo HTTP/1.1\r\ntransfer-encoding: chunked\r\nsomething-else: bleh\r\n\r\n5\r\nhel")
      val requestBytes2 = DataBuffer("lo\r\n2\r\nok\r\n0\r\n\r\n")
      val codec         = new StreamHttpServerCodec

      codec.decode(requestBytes1) mustBe Some(
        Head(
          HttpRequestHead(HttpMethod.Get,
                          "/foo",
                          HttpVersion.`1.1`,
                          HttpHeaders.fromString(
                            "transfer-encoding" -> "chunked",
                            "something-else"    -> "bleh"
                          ))))

      codec.decode(requestBytes1) mustBe None
      codec.decode(requestBytes2) mustBe Some(Data(DataBlock("hello")))
      codec.decode(requestBytes2) mustBe Some(Data(DataBlock("ok")))
      codec.decode(requestBytes2) mustBe Some(End)
      codec.decode(requestBytes2) mustBe None
    }

    "encode standard response parts" in {
      val codec = new StreamHttpServerCodec
      val out   = new DynamicOutBuffer(100)
      val resp = HttpResponseHead(HttpVersion.`1.1`,
                                  HttpCodes.OK,
                                  HttpHeaders.fromString("foo" -> "bar", "content-length" -> "10"))
      val expected = "HTTP/1.1 200 OK\r\nfoo: bar\r\ncontent-length: 10\r\n\r\n0123456789"
      codec.encode(Head(resp), out)
      codec.encode(Data(DataBlock("0123456789")), out)
      codec.encode(End, out)
      out.data.asByteString.utf8String mustBe expected
    }

    "encode chunked response parts" in {
      val codec = new StreamHttpServerCodec
      val out   = new DynamicOutBuffer(100)
      val resp = HttpResponseHead(HttpVersion.`1.1`,
                                  HttpCodes.OK,
                                  HttpHeaders.fromString("foo" -> "bar", "transfer-encoding" -> "chunked"))
      val expected =
        "HTTP/1.1 200 OK\r\nfoo: bar\r\ntransfer-encoding: chunked\r\n\r\n5\r\nhello\r\n6\r\nworld!\r\n0\r\n\r\n"
      codec.encode(Head(resp), out)
      codec.encode(Data(DataBlock("hello")), out)
      codec.encode(Data(DataBlock("world!")), out)
      codec.encode(End, out)
      out.data.asByteString.utf8String mustBe expected
    }

    "not allow sending a new head until current message fully sent" in {
      val codec = new StreamHttpServerCodec
      val out   = new DynamicOutBuffer(100)
      codec.encode(Head(HttpResponse.ok("foo").head), out)
      intercept[StreamHttpException] {
        codec.encode(Head(HttpResponse.ok("foo").head), out)
      }
      codec.encode(End, out)
      //now it should work
      codec.encode(Head(HttpResponse.ok("foo").head), out)
    }

    "properly reset" in {
      val codec = new StreamHttpServerCodec
      codec.decode(DataBuffer("GET /foo")) mustBe None
      codec.reset()
      codec.decode(DataBuffer("GET /foo HTTP/1.1\r\n\r\n")).isEmpty mustBe false

    }

  }

}
