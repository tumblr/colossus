package colossus


import org.scalatest._

import akka.util.ByteString

import protocols.http._

class HttpSpec extends WordSpec with MustMatchers{

  "http request" must {
    "encode to bytes" in {
      val head = HttpHead(
        version = HttpVersion.`1.1`,
        url = "/hello",
        method = HttpMethod.Post,
        headers = List("foo" -> "bar")
      )
      val request = HttpRequest(head, Some(ByteString("hello")))

      val expected = "POST /hello HTTP/1.1\r\nfoo:bar\r\n\r\nhello"

      request.bytes.utf8String must equal(expected)
    }

    "encode request with headers and no body" in {
      val head = HttpHead(
        version = HttpVersion.`1.1`,
        url = "/hello",
        method = HttpMethod.Post,
        headers = List("foo" -> "bar")
      )
      val request = HttpRequest(head, None)

      val expected = "POST /hello HTTP/1.1\r\nfoo:bar\r\n\r\n"

      request.bytes.utf8String must equal(expected)
    }
      
  }
}

