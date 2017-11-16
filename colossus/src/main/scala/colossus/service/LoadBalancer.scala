package colossus.service

import java.net.InetSocketAddress

import colossus.core.{NoRetry, WorkerRef}
import colossus.metrics.logging.ColossusLogging

import scala.concurrent.duration._
import scala.language.higherKinds

private class LoadBalancer[P <: Protocol, T <: Sender[P, Callback]](
    config: ClientConfig,
    clientFactory: ServiceClientFactory[P])(implicit worker: WorkerRef)
    extends Client[P, Callback]
    with ColossusLogging {

  implicit val cbe: CallbackExecutor = worker.callbackExecutor

  private var clients      = config.address.map(address => clientFactory.createClient(config, address))
  private val permutations = new PermutationGenerator(clients)
  private var interceptors = Seq.empty[Interceptor[P]]

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

  // TODO is this just used for tests?
  override def connectionStatus = {
    clients.head.connectionStatus
  }

  override def address() = throw new NotImplementedError("Load balancer does not have an address")

  override def addInterceptor(interceptor: Interceptor[P]): Unit = {
    // save interceptors so they can be added when new clients need to be added
    interceptors = interceptors :+ interceptor
    clients.foreach(_.addInterceptor(interceptor))
  }

  override def update(addresses: Seq[InetSocketAddress]): Unit = {
    // TODO do a diff, remove missing clients, add new clients
  }
}
