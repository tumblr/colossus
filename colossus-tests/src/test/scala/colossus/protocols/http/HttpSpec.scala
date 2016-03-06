package colossus
package protocols.http


import core._
import org.scalatest._

import akka.util.ByteString


class HttpSpec extends WordSpec with MustMatchers{

  import HttpHeader.Conversions._

  "http request" must {
    "encode to bytes" in {
      val head = HttpRequestHead(
        version = HttpVersion.`1.1`,
        url = "/hello",
        method = HttpMethod.Post,
        headers = List("foo" -> "bar")
      )
      val request = HttpRequest(head, Some(ByteString("hello")))

      val expected = "POST /hello HTTP/1.1\r\nfoo: bar\r\n\r\nhello"

      request.bytes.utf8String must equal(expected)
    }

    "encode request with headers and no body" in {
      val head = HttpRequestHead(
        version = HttpVersion.`1.1`,
        url = "/hello",
        method = HttpMethod.Post,
        headers = List("foo" -> "bar")
      )
      val request = HttpRequest(head, None)

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
      val request = HttpRequest(head, None)

      request.head.persistConnection must equal(false)
    }

    "want to close HTTP/1.0 requests without keep-alive in the Connection header" in {
      val head = HttpRequestHead(
        version = HttpVersion.`1.0`,
        url = "/hello",
        method = HttpMethod.Post,
        headers = List("connection" -> "bar")
      )
      val request = HttpRequest(head, None)

      request.head.persistConnection must equal(false)
    }

    "want to persist HTTP/1.0 requests with keep-alive in the Connection header" in {
      val head = HttpRequestHead(
        version = HttpVersion.`1.0`,
        url = "/hello",
        method = HttpMethod.Post,
        headers = List("connection" -> "keep-alive")
      )
      val request = HttpRequest(head, None)

      request.head.persistConnection must equal(true)
    }

    "want to persist HTTP/1.1 requests without the Connection header" in {
      val head = HttpRequestHead(
        version = HttpVersion.`1.1`,
        url = "/hello",
        method = HttpMethod.Post,
        headers = List("foo" -> "bar")
      )
      val request = HttpRequest(head, None)

      request.head.persistConnection must equal(true)
    }

    "want to persist HTTP/1.1 requests without close in the Connection header" in {
      val head = HttpRequestHead(
        version = HttpVersion.`1.1`,
        url = "/hello",
        method = HttpMethod.Post,
        headers = List("connection" -> "bar")
      )
      val request = HttpRequest(head, None)

      request.head.persistConnection must equal(true)
    }

    "want to close HTTP/1.1 requests with close in the Connection header" in {
      val head = HttpRequestHead(
        version = HttpVersion.`1.1`,
        url = "/hello",
        method = HttpMethod.Post,
        headers = List("connection" -> "close")
      )
      val request = HttpRequest(head, None)

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

