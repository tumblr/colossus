package colossus.protocols.http

import colossus.service._

import scala.language.higherKinds

trait BaseHttpClient[M[_], B, P <: BaseHttp[B]] extends LiftedClient[P, M] {

  def ops: MessageOps[HttpRequestHead, B, P#Request] // = implicitly[MessageOps[HttpRequestHead, B, P#Request]]
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
