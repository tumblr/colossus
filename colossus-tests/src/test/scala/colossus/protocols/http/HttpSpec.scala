package colossus


import colossus.core.{DataBuffer, DataStream}
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

      val expected = "POST /hello HTTP/1.1\r\nfoo: bar\r\n\r\nhello"

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

      val expected = "POST /hello HTTP/1.1\r\nfoo: bar\r\n\r\n"

      request.bytes.utf8String must equal(expected)
    }
      
  }

  "http response" must {
    "encode basic response" in {
      val content = "Hello World!"
      val response = HttpResponse(HttpVersion.`1.1`, HttpCodes.OK, Nil, ByteString(content))
      val expected = s"HTTP/1.1 200 OK\r\nContent-Length: ${content.length}\r\n\r\n$content"
      val res = HttpResponseDataReader(response)
      res match {
        case x : DataBuffer => {
          val received = ByteString(x.takeAll).utf8String
          received must equal (expected)
        }
        case y => throw new Exception(s"expected a DataBuffer, received a $y instead")
      }
    }

    "encode a basic response as a stream" in {
      val content = "Hello World!"
      val response = HttpResponse(HttpVersion.`1.1`, HttpCodes.OK, Nil, ByteString(content))
      val expected = s"HTTP/1.1 200 OK\r\nContent-Length: ${content.length}\r\n\r\n$content"
      val stream: DataStream = StreamedResponseBuilder(StreamingHttpResponse.fromStatic(response))
    }
  }
}

