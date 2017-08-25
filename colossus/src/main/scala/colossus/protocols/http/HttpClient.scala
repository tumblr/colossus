package colossus.protocols.http
import colossus.service._

import scala.language.higherKinds

trait BaseHttpClient[M[_], B, P <: BaseHttp[B]] extends LiftedClient[P, M] {

  def ops: MessageOps[HttpRequestHead, B, P#Request] // = implicitly[MessageOps[HttpRequestHead, B, P#Request]]

  protected lazy val hostHeader = clientConfig.map(config => {
    HttpHeader(HttpHeaders.Host, config.address.getHostName)
  })

  override def send(input: P#Request): M[P#Response] = {
    val headers = input.head.headers
    val decoratedRequest = headers.firstValue(HttpHeaders.Host) match {
      case Some(_) => input // Host header is already present.
      case None    => hostHeader.map(header => ops.withHeader(input, header)).getOrElse(input)
    }
    super.send(decoratedRequest)
  }

}

trait HttpClient[M[_]]
    extends LiftedClient[Http, M]
    with BaseHttpClient[M, HttpBody, Http]
    with HttpRequestBuilder[M[HttpResponse]] {

  val ops = HttpRequestOps

  protected def build(req: HttpRequest) = send(req)

  val base = HttpRequest.base

}

object HttpClient {

  implicit object HttpClientLifter extends ClientLifter[Http, HttpClient] {

    override def lift[M[_]](client: Sender[Http, M], clientConfig: Option[ClientConfig])(
        implicit async: Async[M]): HttpClient[M] = {
      new BasicLiftedClient(client, clientConfig) with HttpClient[M]
    }
  }

}
