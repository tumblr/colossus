package colossus
package protocols

import colossus.metrics.TagMap
import colossus.parsing.DataSize
import core.WorkerRef
import service._

import akka.util.ByteString
import scala.concurrent.ExecutionContext

package object http {

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

    override def provideHandler(config: ServiceConfig[Http#Input, Http#Output], worker: WorkerRef, initializer: CodecDSL.HandlerGenerator[Http])
                               (implicit ex: ExecutionContext, tagDecorator: TagDecorator[Http#Input, Http#Output] = TagDecorator.default[Http#Input, Http#Output]): DSLHandler[Http] = {
      new HttpServiceHandler[Http](config, worker, this, initializer)
    }

  }

  implicit object DefaultHttpProvider extends HttpProvider



  object ReturnCodeTagDecorator extends TagDecorator[BaseHttp#Input, BaseHttp#Output] {
    override def tagsFor(request: BaseHttp#Input, response: BaseHttp#Output): TagMap = {
      Map("status_code" -> response.head.code.code.toString)
    }
  }


  class HttpServiceHandler[D <: BaseHttp]
  (config: ServiceConfig[D#Input, D#Output], worker: WorkerRef, provider: CodecProvider[D], initializer: CodecDSL.HandlerGenerator[D])
  (implicit ex: ExecutionContext, tagDecorator: TagDecorator[D#Input, D#Output] = ReturnCodeTagDecorator)
  extends BasicServiceHandler[D](config, worker, provider, initializer) {

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

    override def provideHandler(config: ServiceConfig[StreamingHttp#Input, StreamingHttp#Output], worker: WorkerRef, initializer: CodecDSL.HandlerGenerator[StreamingHttp])
                               (implicit ex: ExecutionContext, tagDecorator: TagDecorator[StreamingHttp#Input, StreamingHttp#Output] = TagDecorator.default[StreamingHttp#Input, StreamingHttp#Output]): DSLHandler[StreamingHttp] = {
      new HttpServiceHandler[StreamingHttp](config, worker, this, initializer)
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
