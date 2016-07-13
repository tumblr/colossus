package colossus
package protocols

import colossus.metrics.TagMap
import core.{InitContext, Server, ServerContext, ServerRef, WorkerRef}
import service._


package object http extends HttpBodyEncoders with HttpBodyDecoders {

  class InvalidRequestException(message: String) extends Exception(message)

  trait Http extends Protocol {
    type Request = HttpRequest
    type Response = HttpResponse
  }

  object Http extends ClientFactories[Http, HttpClient] {

    implicit lazy val clientFactory = ServiceClientFactory.staticClient("http", () => new StaticHttpClientCodec)

    class ServerDefaults  {
      def errorResponse(error: ProcessingFailure[HttpRequest]) = error match {
        case RecoverableError(request, reason) => reason match {
          case c: UnhandledRequestException => request.notFound(s"No route for ${request.head.url}")
          case other => request.error(reason.toString)
        }
        case IrrecoverableError(reason) => {
          HttpResponse(HttpResponseHead(HttpVersion.`1.1`, HttpCodes.BAD_REQUEST,  HttpHeaders.Empty), HttpBody("Bad Request"))
        }
      }
    }

    class ClientDefaults  {
      def name = "http"
    }

    object defaults  {
      
      implicit val httpServerDefaults = new ServerDefaults

      implicit val httpClientDefaults = new ClientDefaults

    }
  }

  class ReturnCodeTagDecorator extends TagDecorator[Http] {
    override def tagsFor(request: HttpRequest, response: HttpResponse): TagMap = {
      Map("status_code" -> response.head.code.code.toString)
    }
  }

}
