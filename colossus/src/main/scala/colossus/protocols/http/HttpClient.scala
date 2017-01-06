package colossus
package protocols.http
import scala.language.higherKinds

import service._
import scala.concurrent.{ExecutionContext, Future}

trait BaseHttpClient[M[_], B, P <: BaseHttp[B]] extends LiftedClient[P, M] {

  private lazy val hostHeader = clientConfig.map(config => {
    HttpHeader(HttpHeaders.Host, config.address.getHostName)
  })

  def ops: MessageOps[HttpRequestHead, B, P#Request]

  override def send(input: P#Request): M[P#Response] = {
    val headers = input.head.headers
    val decoratedRequest: P#Request = headers.firstValue(HttpHeaders.Host) match {
      case Some(_) => input // Host header is already present.
      case None => hostHeader.map(header => ops.withHeader(input, header)).getOrElse(input)
    }
    super.send(decoratedRequest)
  }

}

trait HttpClient[M[_]]  extends LiftedClient[Http, M] with BaseHttpClient[M, HttpBody] with HttpRequestBuilder[M[HttpResponse]]{ 


  protected def build(req: HttpRequest) = send(req)

  val base = HttpRequest.base

}




object HttpClient {

  implicit object HttpClientLifter extends ClientLifter[Http, HttpClient] {
    
    def lift[M[_]](client: Sender[Http,M], clientConfig: Option[ClientConfig])(implicit async: Async[M]) = {
      new BasicLiftedClient(client, clientConfig) with HttpClient[M]
    }
  }

}

trait StreamingHttpClient extends LiftedClient[StreamingHttp, Callback] with BaseClient[Callback, Source[Data]]

/*
object StreamingHttpClient {
    
  implicit object StreamingHttpClientLifter extends ClientLifter[
}
*/
