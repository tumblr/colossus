package colossus.service

import colossus.core.NoRetry
import colossus.metrics.logging.ColossusLogging

import scala.concurrent.duration._
import scala.language.higherKinds

private class LoadBalancer[P <: Protocol, T <: Sender[P, Callback]](clients: Seq[T], config: ClientConfig)(
    implicit cbe: CallbackExecutor)
    extends Client[P, Callback]
    with ColossusLogging {

  private val permutations = new PermutationGenerator(clients)

  override def send(request: P#Request): Callback[P#Response] = {
    if (clients.size == 1 && config.loadBalancerSettings.retryPolicy == NoRetry) {
      clients.head.send(request)
    } else {
      // TODO write logic
      val retryList = permutations.next().take(3)

      def go(next: Sender[P, Callback], list: List[Sender[P, Callback]], attempt: Int): Callback[P#Response] = {
        next.send(request).recoverWith {
          case throwable =>
            list match {
              case head :: tail =>
                Callback.schedule(1.second)(go(head, tail, attempt + 1))
              case Nil =>
                Callback.failed(new SendFailedException(retryList.size, throwable))
            }
        }
      }

      if (retryList.isEmpty) {
        Callback.failed(new SendFailedException(retryList.size, new Exception("Empty client list!")))
      } else {
        go(retryList.head, retryList.tail, 1)
      }
    }
  }

  override def disconnect(): Unit = {
    clients.foreach(_.disconnect())
  }

  // TODO does this need to be a client
  override def connectionStatus = ???

  override def address() = ???

  override def addInterceptor(interceptor: Interceptor[P]): Unit = ???
}

class SendFailedException(tries: Int, finalCause: Throwable)
    extends Exception(
      s"Failed after ${tries} tries, error on last try: ${finalCause.getMessage}",
      finalCause
    )
