package colossus.testkit

import scala.language.higherKinds
import colossus.IOSystem
import colossus.core.WorkerRef
import colossus.service._
import scala.concurrent.Future

object MockSender {

  def apply[P <: Protocol, M[_]](responder: P#Request => M[P#Response]): Sender[P, M] = new Sender[P, M] {
    def send(request: P#Request): M[P#Response] = responder(request)

    def disconnect() {}
  }
}

object MockClientFactory {

  def apply[P <: Protocol, M[_], E](responder: P#Request => M[P#Response]): ClientFactory[P, M, Sender[P, M], E] =
    new ClientFactory[P, M, Sender[P, M], E] {

      def apply(config: ClientConfig)(implicit env: E) = MockSender[P, M](responder)

      def defaultName = "mock-client"
    }

  def client[P <: Protocol](
      responder: P#Request => Callback[P#Response]): ClientFactory[P, Callback, Sender[P, Callback], WorkerRef] =
    apply(responder)

  def futureClient[P <: Protocol](
      responder: P#Request => Future[P#Response]): ClientFactory[P, Future, Sender[P, Future], IOSystem] =
    apply(responder)
}
