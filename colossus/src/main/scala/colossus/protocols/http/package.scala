package colossus.protocols

import colossus.core.DataOutBuffer
import colossus.metrics.TagMap
import colossus.protocols.http.HttpClient.HttpClientLifter
import colossus.service._

package object http {

  class InvalidRequestException(message: String) extends Exception(message)

  trait BaseHttpMessage[H <: HttpMessageHead, B] {
    def head: H
    def body: B
  }

  trait BaseHttp[B] extends Protocol {
    type Request <: BaseHttpMessage[HttpRequestHead, B]
    type Response <: BaseHttpMessage[HttpResponseHead, B]
  }

  trait Http extends BaseHttp[HttpBody] {
    type Request  = HttpRequest
    type Response = HttpResponse
  }

  object Http extends ClientFactories[Http, HttpClient](HttpClientLifter) {

    def requestEnhancement: (HttpRequest, Sender[Http, Callback]) => HttpRequest =
      (input: HttpRequest, sender: Sender[Http, Callback]) => {
        input.head.headers.firstValueInternal(HttpHeaders.Host) match {
          case Some(_) => input // Host header is already present.
          case None    => HttpRequestOps.withHeader(input, HttpHeader(HttpHeaders.Host, sender.address().getHostName))
        }
      }

    implicit lazy val clientFactory =
      ServiceClientFactory.basic[Http]("http", () => new HttpClientCodec, enhancementFunc = requestEnhancement)

    class ServerDefaults {
      def errorResponse(error: ProcessingFailure[HttpRequest]): HttpResponse = error match {
        case RecoverableError(request, reason) =>
          reason match {
            case _: UnhandledRequestException => request.notFound("Not found")
            case _                            => request.error(reason.toString)
          }

        case IrrecoverableError(_) =>
          HttpResponse(HttpResponseHead(HttpVersion.`1.1`, HttpCodes.BAD_REQUEST, HttpHeaders.Empty),
                       HttpBody("Bad Request"))
      }
    }

    class ClientDefaults {
      def name = "http"
    }

    object defaults {

      implicit val httpServerDefaults = new ServerDefaults

      implicit val httpClientDefaults = new ClientDefaults

    }
  }

  trait HttpMessage[H <: HttpMessageHead] extends BaseHttpMessage[H, HttpBody]

  /**
    * common methods of both request and response heads
    */
  trait HttpMessageHead {
    def headers: HttpHeaders
    def version: HttpVersion
    def encode(out: DataOutBuffer)
  }

  trait HeadOps[H <: HttpMessageHead] {
    def withHeader(head: H, header: HttpHeader): H
    def withHeader(head: H, key: String, value: String): H = withHeader(head, HttpHeader(key, value))
  }

  implicit object RequestHeadOps extends HeadOps[HttpRequestHead] {
    def withHeader(head: HttpRequestHead, header: HttpHeader) = head.withHeader(header)
  }

  implicit object ResponseHeadOps extends HeadOps[HttpResponseHead] {
    def withHeader(head: HttpResponseHead, header: HttpHeader) = head.withHeader(header)
  }

  abstract class MessageOps[H <: HttpMessageHead: HeadOps, B, M <: BaseHttpMessage[H, B]](builder: (H, B) => M) {
    def withHeader(message: M, header: HttpHeader): M =
      builder(implicitly[HeadOps[H]].withHeader(message.head, header), message.body)
  }

  implicit object HttpRequestOps extends MessageOps[HttpRequestHead, HttpBody, HttpRequest](HttpRequest.apply _)

  class ReturnCodeTagDecorator extends TagDecorator[Http] {
    override def tagsFor(request: HttpRequest, response: HttpResponse): TagMap = {
      Map("status_code" -> response.head.code.code.toString)
    }
  }

}
