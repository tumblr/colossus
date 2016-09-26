package colossus.examples

import colossus._

import protocols.http._
import stream._
import core.{DataBlock, ServerContext}
import controller._
import service._

import scala.util.{Failure, Success, Try}

object StreamServiceExample {

  def start(port: Int)(implicit sys: IOSystem) = {
    val bodydata = Data(DataBlock("Hello World!"))
    val headers = HttpHeaders(HttpHeader("Content-Length", bodydata.data.length.toString))

    val config = ServiceConfig.Default.copy(
      requestMetrics = false
    )

    StreamHttpServiceServer.basic("stream-service", port, new GenRequestHandler[StreamingHttp](config, _) {

      def handle = {
        case StreamingHttpRequest(head, source) if (head.url == "/plaintext") => source.collected.map{_ =>
          StreamingHttpResponse(
            HttpResponseHead(head.version, HttpCodes.OK, headers),
            Source.one(bodydata)
          )
        }

        case StreamingHttpRequest(head, source) if (head.url == "/chunked") => source.collected.map{_ => 
          StreamingHttpResponse(
            HttpResponseHead(head.version, HttpCodes.OK,  HttpHeaders(HttpHeader("transfer-encoding",TransferEncoding.Chunked.value))), 
            Source.fromIterator(List("hello", "world", "blah").toIterator.map{s => Data(DataBlock(s))})
          )
        }
      }
    })

  }
}

