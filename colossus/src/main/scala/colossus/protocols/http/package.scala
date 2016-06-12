package colossus
package protocols

import colossus.metrics.TagMap
import core.{Server, ServerContext, ServerRef, WorkerRef}
import controller._
import service._


package object http extends HttpBodyEncoders with HttpBodyDecoders {


  class InvalidRequestException(message: String) extends Exception(message)

  trait BaseHttp extends Protocol {
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


  object Http extends ClientFactories[Http, HttpClient] {

    class ServerDefaults extends ServiceCodecProvider[Http] {
      def provideCodec = new HttpServerCodec
      def errorResponse(error: ProcessingFailure[HttpRequest]) = error match {
        case RecoverableError(request, reason) => reason match {
          case c: UnhandledRequestException => request.notFound(s"No route for ${request.head.url}")
          case other => request.error(reason.toString)
        }
        case IrrecoverableError(reason) =>
          HttpResponse(HttpResponseHead(HttpVersion.`1.1`, HttpCodes.BAD_REQUEST,  HttpHeaders.Empty), HttpBody("Bad Request"))
      }
        
        

    }

    class ClientDefaults extends ClientCodecProvider[Http] {
      def clientCodec = new HttpClientCodec
      def name = "http"
    }

    object defaults  {
      
      implicit val httpServerDefaults = new ServerDefaults

      implicit val httpClientDefaults = new ClientDefaults

    }
  }

  class ReturnCodeTagDecorator[C <: BaseHttp] extends TagDecorator[C#Input, C#Output] {
    override def tagsFor(request: C#Input, response: C#Output): TagMap = {
      Map("status_code" -> response.head.code.code.toString)
    }
  }

  class HttpServiceHandler(rh: RequestHandler[Http]) 
  extends BasicServiceHandler[Http](rh) {

    //TODO: take as paramter
    val defaults = new Http.ServerDefaults

    val codec = new HttpServerCodec

    override def tagDecorator = new ReturnCodeTagDecorator[Http]

    override def processRequest(input: Http#Input): Callback[Http#Output] = {
      val response = super.processRequest(input)
      if(!input.head.persistConnection) disconnect()
      response
    }
    def unhandledError = {
      case (error) => defaults.errorResponse(error)
    }

    def receivedMessage(message: Any, sender: akka.actor.ActorRef){}

  }

  abstract class Initializer(worker: WorkerRef) {

    def onConnect(ctx: ServerContext): RequestHandler[Http]

  }

  abstract class RequestHandler(config: ServiceConfig, ctx: ServerContext) extends GenericRequestHandler[Http](config, ctx) {
    def this(ctx: ServerContext) = this(ServiceConfig.load(ctx.name), ctx)
  }

  object HttpServer {
    
    def start(name: String, port: Int)(init: WorkerRef => Initializer)(implicit io: IOSystem): ServerRef = {
      Server.start(name, port){worker => new core.Initializer(worker) {
        val httpInitializer = init(worker)
        def onConnect = ctx => new HttpServiceHandler(httpInitializer.onConnect(ctx))
      }}
    }
  }
  /*

  implicit object StreamingHttpProvider extends ServiceCodecProvider[StreamingHttp] {
    def provideCodec = new StreamingHttpServerCodec
    def errorResponse(error: ProcessingFailure[HttpRequest]) = error match {
      case RecoverableError(request, reason) => reason match {
        case c: UnhandledRequestException => toStreamed(request.notFound(s"No route for ${request.head.url}"))
        case other => toStreamed(request.error(reason.toString))
      }
      case IrrecoverableError(reason) =>
        toStreamed(
          HttpResponse(HttpResponseHead(HttpVersion.`1.1`, HttpCodes.BAD_REQUEST,  HttpHeaders.Empty), HttpBody("Bad Request"))
        )
      
    }
      

    private def toStreamed(response : HttpResponse) : StreamingHttpResponse = {
      StreamingHttpResponse.fromStatic(response)
    }

  }

  implicit object StreamingHttpClientProvider extends ClientCodecProvider[StreamingHttp] {

    def clientCodec = new StreamingHttpClientCodec
    def name = "streamingHttp"
  }


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

}
