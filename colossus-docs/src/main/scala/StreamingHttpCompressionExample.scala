import akka.actor.ActorSystem
import colossus.core._
import colossus.protocols.http._
import colossus.protocols.http.filters.HttpStreamCustomFilters
import colossus.protocols.http.streaming._
import colossus.service._
import colossus.streaming._

object StreamingHttpCompressionExample extends App {

  // #streaming_http

  class MyRequestHandler(serverContext: ServerContext) extends GenRequestHandler[StreamingHttp](serverContext) {

    def handle = {
      case StreamingHttpRequest(head, source) if (head.url == "/chunked") => {

        source.collected.map { sourceBody =>
          val responseBody = Source.fromIterator(List("this is ", "a chunked ", "response").toIterator.map { s =>
            Data(DataBlock(s))
          })
          StreamingHttpResponse(
            HttpResponseHead(head.version,
                             HttpCodes.OK,
                             Some(TransferEncoding.Chunked),
                             None,
                             None,
                             None,
                             HttpHeaders.Empty),
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
    StreamingHttpServer.basic(
      "stream-service",
      port,
      serverContext =>
        new MyRequestHandler(serverContext) {
          //enable Gzip/deflate compression
          override def filters: Seq[Filter[StreamingHttp]] = Seq(new HttpStreamCustomFilters.CompressionFilter())
      }
    )
  }

  implicit val actorSystem = ActorSystem("stream")
  start(9000)(IOSystem())
  // #streaming_http

}
