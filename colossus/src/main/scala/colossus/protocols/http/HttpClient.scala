package colossus
package protocols.http
import scala.language.higherKinds

import core._
import service._
import scala.concurrent.{ExecutionContext, Future}

trait HttpClient[M[_]] extends ResponseAdapter[Http, M] {

}

object HttpClient {

  implicit object ServiceClientLifter extends ServiceClientLifter[Http, HttpClient[Callback]] {

    def lift(client: CodecClient[Http])(implicit worker: WorkerRef): HttpClient[Callback] = {
      new LiftedCallbackClient(client) with HttpClient[Callback]
    }
  }

  implicit object FutureClientLifter extends FutureClientLifter[Http, HttpClient[Future]] {
    def lift(client: FutureClient[Http])(implicit io: IOSystem): HttpClient[Future] = {
      import io.actorSystem.dispatcher
      new LiftedFutureClient(client) with HttpClient[Future]
    }
  }

}
