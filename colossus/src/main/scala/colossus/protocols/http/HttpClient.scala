package colossus
package protocols.http
import scala.language.higherKinds

import service._
import scala.concurrent.{ExecutionContext, Future}

trait HttpClient[M[_]]  extends LiftedClient[Http, M] with HttpRequestBuilder[M[HttpResponse]]{ //self: LiftedClient[Http, M] => 

  private lazy val hostHeader = clientConfig.map(config => {
    HttpHeader(HttpHeaders.Host, config.address.getHostName)
  })

  protected def build(req: HttpRequest) = send(req)

  val base = HttpRequest.base

  override def send(input: HttpRequest): M[HttpResponse] = {
    val headers = input.head.headers
    val decoratedRequest = headers.firstValue(HttpHeaders.Host) match {
      case Some(_) => input // Host header is already present.
      case None => hostHeader.map(header => input.withHeader(header)).getOrElse(input)
    }
    super.send(decoratedRequest)
  }
}



object HttpClient {

  implicit object HttpClientLifter extends ClientLifter[Http, HttpClient] {
    
    def lift[M[_]](client: Sender[Http,M], clientConfig: Option[ClientConfig])(implicit async: Async[M]) = {
      new BasicLiftedClient(client, clientConfig) with HttpClient[M]
    }
  }

}
