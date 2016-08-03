package colossus
package protocols.http
import scala.language.higherKinds

import service._
import scala.concurrent.{ExecutionContext, Future}

trait HttpClient[M[_]]  extends LiftedClient[Http, M] with HttpRequestBuilder[M[HttpResponse]]{ //self: LiftedClient[Http, M] => 

  protected def build(req: HttpRequest) = client.send(req)

  val base = HttpRequest.base


}



object HttpClient {

  implicit object HttpClientLifter extends ClientLifter[Http, HttpClient] {
    
    def lift[M[_]](client: Sender[Http,M])(implicit async: Async[M]) = new BasicLiftedClient(client) with HttpClient[M]
  }

}
