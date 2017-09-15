import colossus._

import protocols.http._
import protocols.http.streaming._
import core.DataBlock
import service._
import colossus.streaming._

object StreamServiceExample {

  // #streaming_http

  class MyRequestHandler extends GenRequestHandler[StreamingHttp](serverContext, config) {

    def handle = {
      case StreamingHttpRequest(head, source) if (head.url == "/chunked") => {
        source.collected.map { sourceBody =>
          val responseBody = Source.fromIterator(List("this is ", "a chunked ", "response").toIterator.map { s =>
            Data(DataBlock(s))
          })
          StreamingHttpResponse(
            HttpResponseHead(head.version, HttpCodes.OK, Some(TransferEncoding.Chunked), None, None, None, HttpHeaders.Empty),
            responseBody
          )
        }
      }
    }

    def unhandledError = {
      case err => StreamingHttpResponse(HttpResponse.error(s"error: $err"))
    }
  }

  def start(port: Int)(implicit sys: IOSystem) = {
    StreamingHttpServer.basic("stream-service", port, serverContext => new MyRequestHandler)
  }

  // #streaming_http
}

