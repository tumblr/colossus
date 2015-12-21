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


  trait Http extends CodecDSL {
    type Input = HttpRequest
    type Output = HttpResponse
  }

  trait HttpProvider extends CodecProvider[Http] {
    def provideCodec = new HttpServerCodec
    def errorResponse(request: HttpRequest, reason: Throwable) = reason match {
      case c: UnhandledRequestException => request.notFound(s"No route for ${request.head.url}")
      case other => request.error(reason.toString)
    }

    override def provideHandler(config: ServiceConfig[Http#Input, Http#Output], worker: WorkerRef, initializer: CodecDSL.HandlerGenerator[Http])
                               (implicit ex: ExecutionContext, tagDecorator: TagDecorator[Http#Input, Http#Output] = new ReturnCodeTagDecorator): DSLHandler[Http] = {
      new HttpServiceHandler(config, worker, this, initializer)
    }

  }

  implicit object DefaultHttpProvider extends HttpProvider



  class ReturnCodeTagDecorator extends TagDecorator[HttpRequest, HttpResponse] {
    override def tagsFor(request: HttpRequest, response: HttpResponse): TagMap = {
      Map("status_code" -> response.head.code.code.toString)
    }
  }


  class HttpServiceHandler
  (config: ServiceConfig[HttpRequest, HttpResponse], worker: WorkerRef, provider: CodecProvider[Http], initializer: CodecDSL.HandlerGenerator[Http])
  (implicit ex: ExecutionContext, tagDecorator: TagDecorator[HttpRequest, HttpResponse])
  extends BasicServiceHandler[Http](config, worker, provider, initializer) {

    override def processRequest(input: HttpRequest): Callback[HttpResponse] = super.processRequest(input).map{response =>
      if(!input.head.persistConnection) gracefulDisconnect()
      response
    }

  }

  implicit object HttpClientProvider extends ClientCodecProvider[Http] {
    def clientCodec = new HttpClientCodec
    def name = "http"
  }


  class HttpClient(config : ClientConfig, worker: WorkerRef, maxSize : DataSize = HttpResponseParser.DefaultMaxSize)
    extends ServiceClient[HttpRequest, HttpResponse](
      codec = new HttpClientCodec,//(maxSize),
      config = config,
      worker = worker
    )


  implicit object ByteStringLikeString extends ByteStringLike[String] {
    override def toByteString(t: String): ByteString = ByteString(t)
  }

  implicit object ByteStringLikeByteString extends ByteStringLike[ByteString] {
    override def toByteString(t: ByteString): ByteString = t
  }

}
