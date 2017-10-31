package colossus.service

import colossus.metrics.logging.ColossusLogging

import scala.concurrent.duration._
import scala.language.higherKinds

trait LBC {
  def addClient(host: String, port: Int): Unit
  def removeClient(host: String, port: Int, allInstances: Boolean): Unit
}

case class LoadBalanceClient[P <: Protocol, M[_], +T <: Sender[P, M], E](
    hostManager: HostManager,
    clientFactory: ClientFactory[P, M, T, E])(implicit env: E, async: Async[M])
    extends Sender[P, M]
    with LBC
    with ColossusLogging {

  case class ClientWrapper(client: Sender[P, M], host: String, port: Int)

  private var clients = Seq.empty[ClientWrapper]

  private val retryPolicy = hostManager.retryPolicy

  private var permutations = new PermutationGenerator(clients.map { _.client })

  // first register with the host manager, which acts as the interface between the user and this class
  hostManager.attachLoadBalancer(this)

  def addClient(host: String, port: Int): Unit = {
    clients = ClientWrapper(clientFactory(host, port), host, port) +: clients
    regeneratePermutations()
  }

  // TODO will this fuck up requests in progress?
  def removeClient(host: String, port: Int, allInstances: Boolean): Unit = {
    if (allInstances) {
      val (removedClients, keepClients) =
        clients.partition(clientWrapper => clientWrapper.host == host && clientWrapper.port == port)
      // disconnect the old clients
      removedClients.foreach(_.client.disconnect())

      clients = keepClients
    } else {
      ???
    }
    regeneratePermutations()
  }

  private def regeneratePermutations() {
    permutations = new PermutationGenerator(clients.map { _.client })
  }

  def send(request: P#Request): M[P#Response] = {

    retryPolicy match {
      case LBRetryPolicy(Random, backoff, attempts) => ???

      case LBRetryPolicy(WithoutReplacement, backoff, attempts) =>
        val maxTries = attempts match {
          case MaxAttempts(max)      => max
          case EveryHost(upperLimit) => clients.size.min(upperLimit)
        }

        val retryList = permutations.next().take(maxTries)

        def go(next: Sender[P, M], list: List[Sender[P, M]], attempt: Int): M[P#Response] = {
          async.recoverWith(next.send(request)) {
            case throwable =>
              list match {
                case head :: tail =>
                  async.delay(backoffDuration(backoff, attempt))(go(head, tail, attempt + 1))
                case Nil =>
                  async.failure(new SendFailedException(retryList.size, throwable))
              }
          }
        }

        if (retryList.isEmpty) {
          async.failure(new SendFailedException(retryList.size, new Exception("Empty client list!")))
        } else {
          val f = async.failure(new SendFailedException(retryList.size, new Exception("HAHA")))
          go(retryList.head, retryList.tail, 1)
        }
    }
  }

  private def backoffDuration(backoff: LBBackoff, attempt: Int): FiniteDuration = {
    backoff match {
      case LinearBackoff(pause) => pause
      case ExponentialBackoff(initial, max) =>
        val calculatedBackoff = initial * attempt
        if (calculatedBackoff > max) {
          max
        } else {
          calculatedBackoff
        }
    }
  }

  def disconnect(): Unit = {
    clients.foreach(_.client.disconnect())
  }

}
