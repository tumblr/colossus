package colossus
package protocols

import colossus.metrics.TagMap
import colossus.parsing.DataSize
import core.{Context, ServerContext, WorkerRef}
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

  }

  implicit object DefaultHttpProvider extends HttpProvider



  class ReturnCodeTagDecorator[C <: BaseHttp] extends TagDecorator[C#Input, C#Output] {
    override def tagsFor(request: C#Input, response: C#Output): TagMap = {
      Map("status_code" -> response.head.code.code.toString)
    }
  }

  abstract class BaseHttpServiceHandler[D <: BaseHttp]
  (config: ServiceConfig, provider: CodecProvider[D], context: ServerContext)
  extends Service[D](config, context)(provider) {

    override def tagDecorator = new ReturnCodeTagDecorator

    override def processRequest(input: D#Input): Callback[D#Output] = super.processRequest(input).map{response =>
      if(!input.head.persistConnection) disconnect()
      response
    }

  }

  abstract class HttpService(config: ServiceConfig, context: ServerContext) extends BaseHttpServiceHandler[Http](config, DefaultHttpProvider, context)

  abstract class StreamingHttpService(config: ServiceConfig, context: ServerContext) extends BaseHttpServiceHandler[StreamingHttp](config, StreamingHttpProvider, context)

  implicit object StreamingHttpProvider extends CodecProvider[StreamingHttp] {
    def provideCodec = new StreamingHttpServerCodec
    def errorResponse(request: HttpRequest, reason: Throwable) = reason match {
      case c: UnhandledRequestException => toStreamed(request.notFound(s"No route for ${request.head.url}"))
      case other => toStreamed(request.error(reason.toString))
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


  class HttpClient(config : ClientConfig, context: Context)
    extends ServiceClient[HttpRequest, HttpResponse](new HttpClientCodec,config, context)


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
