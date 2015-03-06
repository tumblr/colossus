package colossus
package protocols

import colossus.parsing.DataSize
import core.WorkerRef
import service._

import akka.util.ByteString
import scala.concurrent.ExecutionContext

package object http {

  class HttpParsingException(message: String) extends Exception(message)

  class InvalidRequestException(message: String) extends Exception(message)

  trait BaseHttp extends CodecDSL {
    type Input <: HttpRequest
    type Output <: BaseHttpResponse
  }


  trait Http extends BaseHttp {
    type Input = HttpRequest
    type Output = HttpResponse
  }

  trait StreamingHttp extends BaseHttp {
    type Input = HttpRequest
    type Output = StreamingHttpResponse
  }

  trait HttpProvider extends CodecProvider[Http] {
    def provideCodec = new HttpServerCodec
    def errorResponse(request: HttpRequest, reason: Throwable) = reason match {
      case c: UnhandledRequestException => request.notFound(s"No route for ${request.head.url}")
      case other => request.error(reason.toString)
    }

    override def provideHandler(config: ServiceConfig, worker: WorkerRef)(implicit ex: ExecutionContext): DSLHandler[Http] = {
      new HttpServiceHandler(config, worker, this)
    }

  }

  implicit object DefaultHttpProvider extends HttpProvider

  class HttpServiceHandler[D <: BaseHttp](config: ServiceConfig, worker: WorkerRef, provider: CodecProvider[D])(implicit ex: ExecutionContext) 
  extends BasicServiceHandler(config, worker, provider) {

    override def processRequest(input: D#Input): Callback[D#Output] = super.processRequest(input).map{response =>
      if (response.head.version == HttpVersion.`1.0`) {
        gracefulDisconnect()
      }
      response
    }
    
  }

  implicit object StreamingHttpProvider extends CodecProvider[StreamingHttp] {
    def provideCodec = new StreamingHttpServerCodec
    def errorResponse(request: HttpRequest, reason: Throwable) = reason match {
      case c: UnhandledRequestException => toStreamed(request.notFound(s"No route for ${request.head.url}"))
      case other => toStreamed(request.error(reason.toString))
    }

    override def provideHandler(config: ServiceConfig, worker: WorkerRef)(implicit ex: ExecutionContext): DSLHandler[StreamingHttp] = {
      new HttpServiceHandler(config, worker, this)
    }

    private def toStreamed(response : HttpResponse) : StreamingHttpResponse = {
      StreamingHttpResponse.fromStatic(response)
    }

  }

  implicit object HttpClientProvider extends ClientCodecProvider[Http] {
    def clientCodec = new HttpClientCodec
    def name = "http"
  }


  implicit object StreamingHttpClientProvider extends ClientCodecProvider[StreamingHttp] {

    def clientCodec = new StreamingHttpClientCodec
    def name = "streamingHttp"
  }


  class HttpClient(config : ClientConfig, worker: WorkerRef, maxSize : DataSize = HttpResponseParser.DefaultMaxSize)
    extends ServiceClient[HttpRequest, HttpResponse](
      codec = new HttpClientCodec,//(maxSize),
      config = config,
      worker = worker
    )


  /*
  class StreamingHttpClient(config : ClientConfig,
                            worker : WorkerRef,
                            maxSize : DataSize = HttpResponseParser.DefaultMaxSize,
                            streamBufferSize : Int = HttpResponseParser.DefaultQueueSize)
    extends ServiceClient[HttpRequest, StreamingHttpResponse](
      codec = HttpClientCodec.streaming(maxSize, streamBufferSize),
      config = config,
      worker = worker
    )

    */
  implicit object ByteStringLikeString extends ByteStringLike[String] {
    override def toByteString(t: String): ByteString = ByteString(t)
  }

  implicit object ByteStringLikeByteString extends ByteStringLike[ByteString] {
    override def toByteString(t: ByteString): ByteString = t
  }

}
