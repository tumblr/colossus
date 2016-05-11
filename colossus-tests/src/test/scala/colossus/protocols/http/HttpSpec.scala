package colossus
package protocols.http


import core._
import org.scalatest._

import akka.util.ByteString
import scala.util.Success


class HttpSpec extends WordSpec with MustMatchers{

  import HttpHeader.Conversions._

  "first line" must {

    "respect equality" in {
      val fl1 : FirstLine = HttpRequest(HttpMethod.Get, "/foobar", HttpHeaders(), HttpBody.NoBody).head.firstLine
      val fl2 : FirstLine = ParsedFL(ByteString("GET /foobar HTTP/1.1\t\n").toArray)
      val fl3 : FirstLine = ParsedFL(ByteString("GET /foobaz HTTP/1.1\t\n").toArray)
      fl1 == fl2 must equal(true)
      fl1 == fl3 must equal(false)
      fl1 == "bleh" must equal(false)
    }
  }

  "http body" must {

    "create with encoder and custom content-type" in {
      val str = "hello, world!™ᴂ無奈朝來寒雨"
      val b = HttpBody(str, "foo/bar")
      b.contentType.get.value must equal("foo/bar")
      b.bytes must equal(ByteString(str))
    }

    "copy with new content-type" in {
      val b = HttpBody("hello")
      b.withContentType("foo/bar").contentType.get.value must equal("foo/bar")
    }

    "decode using built-in decoders" in {
      val str = "hello, world!™ᴂ無奈朝來寒雨"
      val b = HttpBody(str)
      b.as[String] must equal(Success(str))
      b.as[ByteString] must equal(Success(ByteString(str)))
      b.as[Array[Byte]].map{_.toSeq} must equal(Success(str.getBytes("UTF-8").toSeq))
    }
  }



  "http request" must {
    "encode to bytes" in {
      val head = HttpRequestHead(
        version = HttpVersion.`1.1`,
        url = "/hello",
        method = HttpMethod.Post,
        headers = List("foo" -> "bar")
      )
      val request = HttpRequest(head, HttpBody(ByteString("hello")))

      val expected = "POST /hello HTTP/1.1\r\nfoo: bar\r\nContent-Length: 5\r\n\r\nhello"

      request.bytes.utf8String must equal(expected)
    }

    "encode request with headers and no body" in {
      val head = HttpRequestHead(
        version = HttpVersion.`1.1`,
        url = "/hello",
        method = HttpMethod.Post,
        headers = List("foo" -> "bar")
      )
      val request = HttpRequest(head, HttpBody.NoBody)

      val expected = "POST /hello HTTP/1.1\r\nfoo: bar\r\n\r\n"

      request.bytes.utf8String must equal(expected)
    }

    "want to close HTTP/1.0 requests without the Connection header" in {
      val head = HttpRequestHead(
        version = HttpVersion.`1.0`,
        url = "/hello",
        method = HttpMethod.Post,
        headers = List("foo" -> "bar")
      )
      val request = HttpRequest(head, HttpBody.NoBody)

      request.head.persistConnection must equal(false)
    }

    "want to close HTTP/1.0 requests without keep-alive in the Connection header" in {
      val head = HttpRequestHead(
        version = HttpVersion.`1.0`,
        url = "/hello",
        method = HttpMethod.Post,
        headers = List("connection" -> "close")
      )
      val request = HttpRequest(head, HttpBody.NoBody)

      request.head.persistConnection must equal(false)
    }

    "want to persist HTTP/1.0 requests with keep-alive in the Connection header" in {
      val head = HttpRequestHead(
        version = HttpVersion.`1.0`,
        url = "/hello",
        method = HttpMethod.Post,
        headers = List("connection" -> "keep-alive")
      )
      val request = HttpRequest(head, HttpBody.NoBody)

      request.head.persistConnection must equal(true)
    }

    "want to persist HTTP/1.1 requests without the Connection header" in {
      val head = HttpRequestHead(
        version = HttpVersion.`1.1`,
        url = "/hello",
        method = HttpMethod.Post,
        headers = List("foo" -> "bar")
      )
      val request = HttpRequest(head, HttpBody.NoBody)

      request.head.persistConnection must equal(true)
    }

    "want to persist HTTP/1.1 requests without close in the Connection header" in {
      val head = HttpRequestHead(
        version = HttpVersion.`1.1`,
        url = "/hello",
        method = HttpMethod.Post,
        headers = List("connection" -> "keep-alive")
      )
      val request = HttpRequest(head, HttpBody.NoBody)

      request.head.persistConnection must equal(true)
    }

    "want to close HTTP/1.1 requests with close in the Connection header" in {
      val head = HttpRequestHead(
        version = HttpVersion.`1.1`,
        url = "/hello",
        method = HttpMethod.Post,
        headers = List("connection" -> "close")
      )
      val request = HttpRequest(head, HttpBody.NoBody)

      request.head.persistConnection must equal(false)
    }

  }

  "http response" must {
    "encode basic response" in {
      val content = "Hello World!"
      val response = HttpResponse(HttpVersion.`1.1`, HttpCodes.OK, HttpHeaders(), ByteString(content))
      val expected = s"HTTP/1.1 200 OK\r\nContent-Length: ${content.length}\r\n\r\n$content"
      val buf = new DynamicOutBuffer(100)
      val res = response.encode(buf)
      val received = ByteString(buf.data.takeAll).utf8String
      received must equal (expected)
    }

    "encode a basic response as a stream" ignore {
      val content = "Hello World!"
      val response = HttpResponse(HttpVersion.`1.1`, HttpCodes.OK, HttpHeaders(), ByteString(content))
      val expected = s"HTTP/1.1 200 OK\r\nContent-Length: ${content.length}\r\n\r\n$content"
      //val stream: DataReader = StreamingHttpResponse.fromStatic(response).encode(new DynamicOutBuffer(100))
    }
  }
}

