package colossus.examples

import colossus._
import protocols.http._
import protocols.http.streaming._
import core.{DataBlock, ServerSettings}
import service._
import colossus.streaming._

object StreamServiceExample {

  def start(port: Int)(implicit sys: IOSystem) = {
    val bodydata = Data(DataBlock("Hello World!"))
    val headers  = HttpHeaders(HttpHeader("Content-Length", bodydata.data.length.toString))
    val body     = HttpBody("Hello World!")

    StreamingHttpServer.basic(
      "stream-service",
      port,
      serverContext =>
        new GenRequestHandler[StreamingHttp](serverContext) {

          def handle = {
            case StreamingHttpRequest(head, source) if (head.url == "/plaintext") =>
              source.collected.map { _ =>
                StreamingHttpResponse(
                  HttpResponse(HttpResponseHead(head.version, HttpCodes.OK, None, None, None, None, headers), body))
              }

            case StreamingHttpRequest(head, source) if (head.url == "/chunked") =>
              source.collected.map { _ =>
                StreamingHttpResponse(
                  HttpResponseHead(head.version,
                                   HttpCodes.OK,
                                   Some(TransferEncoding.Chunked),
                                   None,
                                   None,
                                   None,
                                   HttpHeaders.Empty),
                  Source.fromIterator(List("hello", "world", "blah").toIterator.map { s =>
                    Data(DataBlock(s))
                  })
                )
              }
          }

          def unhandledError = {
            case err => StreamingHttpResponse(HttpResponse.error(s"error: $err"))
          }
      }
    )

  }
}
