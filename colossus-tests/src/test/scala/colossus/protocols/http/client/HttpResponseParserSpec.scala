package colossus
package protocols.http

import colossus.service.DecodedResult
import core.{DataBuffer, DynamicOutBuffer}

import akka.util.ByteString
import org.scalatest.{WordSpec, MustMatchers}

//NOTICE - all expected headers names must lowercase, otherwise these tests will fail equality testing

class HttpResponseParserSpec extends WordSpec with MustMatchers {

  import DecodedResult.Static

  import HttpHeader.Conversions._

  "HttpResponseParser" must {

    "parse a basic response" in {
      val res = ByteString("HTTP/1.1 204 NO_CONTENT\r\n\r\n")
      val parser = HttpResponseParser.static()
      val data = DataBuffer(res)
      parser.parse(data) must equal(Some(Static(HttpResponse(HttpVersion.`1.1`, HttpCodes.NO_CONTENT, HttpHeaders()))))
      data.remaining must equal(0)
    }


    "parse a 200 response with content-length 0" in {

      val res = "HTTP/1.1 200 OK\r\nHost: api.foo.bar:444\r\nAccept: */*\r\nAuthorization: Basic XXX\r\nAccept-Encoding: gzip, deflate\r\ncontent-length: 0\r\n\r\n"
      val parser = HttpResponseParser.static()

      val expected = HttpResponse(
        HttpVersion.`1.1`,
        HttpCodes.OK,
        Seq("host"->"api.foo.bar:444", "accept"->"*/*", "authorization"->"Basic XXX", "accept-encoding"->"gzip, deflate", "content-length"->"0")
      )
      val actual = parser.parse(DataBuffer(ByteString(res))).get.value
      println(actual.head.headers.toSeq.toSet.toString)
      println(expected.head.headers.toSeq.toSet.toString)
      println("ISECT " + actual.head.headers.toSeq.toSet.intersect(expected.head.headers.toSeq.toSet).toString)
      actual.head.headers must equal(expected.head.headers)
      actual must equal(expected)

    }

    "parse a 200 response with body and no content-length" in {

      val body = "hello world"
      val res = s"HTTP/1.1 200 OK\r\nHost: api.foo.bar:444\r\nAccept: */*\r\nAuthorization: Basic XXX\r\nAccept-Encoding: gzip, deflate\r\n\r\n${body}"
      val parser = HttpResponseParser.static()

      val expected = Static(HttpResponse(
        HttpResponseHead(
          HttpVersion.`1.1`,
          HttpCodes.OK,
          Seq("host"->"api.foo.bar:444", "accept"->"*/*", "authorization"->"Basic XXX", "accept-encoding"->"gzip, deflate")
        ),
        HttpBody(body)
      ))
      parser.parse(DataBuffer(ByteString(res))) must equal(None)
      parser.endOfStream() must equal(Some(expected))
    }

    "parse a response with a body" in {
      val content = "{some : json}"
      val size = content.getBytes.size
      val res = s"HTTP/1.1 200 OK\r\nHost: api.foo.bar:444\r\nAccept: */*\r\nAuthorization: Basic XXX\r\nAccept-Encoding: gzip, deflate\r\nContent-Length: $size\r\n\r\n{some : json}"
      val parser = HttpResponseParser.static()

      val expected = Static(HttpResponse(HttpVersion.`1.1`,
        HttpCodes.OK,
        Seq("host"->"api.foo.bar:444", "accept"->"*/*", "authorization"->"Basic XXX", "accept-encoding"->"gzip, deflate", "content-length"->size.toString),
        ByteString("{some : json}")
      ))
      val data = DataBuffer(ByteString(res))
      parser.parse(data).toList must equal(List(expected))
      data.remaining must equal(0)
    }


    "decode a response that was encoded by colossus with no body" in {
      val sent = HttpResponse(
        HttpVersion.`1.1`,
        HttpCodes.OK,
        Seq("host"->"api.foo.bar:444", "accept"->"*/*", "authorization"->"Basic XXX", "accept-encoding"->"gzip, deflate")
      )

      val expected = Some(DecodedResult.Static(sent.withHeader("content-length", "0")))

      val serverProtocol = new HttpServerCodec
      val clientProtocol = new HttpClientCodec

      val buf = new DynamicOutBuffer(100)
      sent.encode(buf)

      val encoded = buf.data
      val decodedResponse = clientProtocol.decode(encoded)
      decodedResponse must equal(expected)
      encoded.remaining must equal(0)
    }

    "decode a response that was encoded by colossus with a body" in {
      val content = "{some : json}"
      val size = content.getBytes.size

      val sent = HttpResponse(
        HttpVersion.`1.1`,
        HttpCodes.OK,
        Seq("host"->"api.foo.bar:444", "accept"->"*/*", "authorization"->"Basic XXX", "accept-encoding"->"gzip, deflate"),
        ByteString("{some : json}")
      )

      val expected = Some(DecodedResult.Static(sent.withHeader("content-length", size.toString)))

      val serverProtocol = new HttpServerCodec
      val clientProtocol = new HttpClientCodec

      val buf = new DynamicOutBuffer(100)
      sent.encode(buf)
      val encodedResponse = buf.data

      val decodedResponse = clientProtocol.decode(encodedResponse)
      decodedResponse must equal(expected)
      encodedResponse.remaining must equal(0)
    }

    "parse a response with chunked transfer encoding" in {
      val res = "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n3\r\nfoo\r\ne\r\n123456789abcde\r\n0\r\n\r\n"
      val parser = HttpResponseParser.static()

      val parsed = parser.parse(DataBuffer(ByteString(res))).get
      parsed.value.body.bytes must equal (ByteString("foo123456789abcde"))
    }


  }


}
