package colossus
package protocols

import colossus.parsing.DataSize
import service._

package object http {

  class HttpParsingException(message: String) extends Exception(message)

  class InvalidRequestException(message: String) extends Exception(message)


  trait Http extends CodecDSL {
    type Input = HttpRequest
    type Output = HttpResponse
  }

  trait StreamingHttp extends CodecDSL {
    type Input = HttpRequest
    type Output = StreamingHttpResponse
  }

  implicit object HttpProvider extends CodecProvider[Http] {
    def provideCodec = HttpServerCodec.static
    def errorResponse(request: HttpRequest, reason: Throwable) = reason match {
      case c: UnhandledRequestException => request.notFound(s"No route for ${request.head.url}")
      case other => request.error(reason.toString)
    }

  }

  implicit object StreamingHttpProvider extends CodecProvider[StreamingHttp] {
    def provideCodec = HttpServerCodec.streaming
    def errorResponse(request: HttpRequest, reason: Throwable) = reason match {
      case c: UnhandledRequestException => toStreamed(request.notFound(s"No route for ${request.head.url}"))
      case other => toStreamed(request.error(reason.toString))
    }

    private def toStreamed(c : Completion[HttpResponse]) : Completion[StreamingHttpResponse] = {
      val streamed = StreamingHttpResponse.fromStatic(c.value)
      c.copy(value = streamed)
    }

  }

  implicit object HttpClientProvider extends ClientCodecProvider[Http] {
    def clientCodec = HttpClientCodec.static()
    def name = "http"
  }

  implicit object StreamingHttpClientProvider extends ClientCodecProvider[StreamingHttp] {

    def clientCodec = HttpClientCodec.streaming()
    def name = "streamingHttp"
  }

  class HttpClient(config : ClientConfig, maxSize : DataSize = HttpResponseParser.DefaultMaxSize)
    extends ServiceClient[HttpRequest, HttpResponse](
      codec = HttpClientCodec.static(maxSize),
      config = config
    )

  class StreamingHttpClient(config : ClientConfig,
                            maxSize : DataSize = HttpResponseParser.DefaultMaxSize,
                            streamBufferSize : Int = HttpResponseParser.DefaultQueueSize)
    extends ServiceClient[HttpRequest, StreamingHttpResponse](
      codec = HttpClientCodec.streaming(maxSize, streamBufferSize),
      config = config
    )

}
