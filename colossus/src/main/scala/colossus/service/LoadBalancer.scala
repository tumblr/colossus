package colossus.service

import java.net.InetSocketAddress

import colossus.core.RetryAttempt.{RetryIn, RetryNow, Stop}
import colossus.core.{BackoffPolicy, ConnectionStatus, NoRetry, WorkerRef}
import colossus.metrics.logging.ColossusLogging

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}

private class LoadBalancer[P <: Protocol](
    config: ClientConfig,
    clientFactory: ServiceClientFactory[P],
    permutationFactory: Seq[Client[P, Callback]] => PermutationGenerator[Client[P, Callback]])(
    implicit worker: WorkerRef)
    extends Client[P, Callback]
    with ColossusLogging {

  implicit val cbe: CallbackExecutor = worker.callbackExecutor
  
  private var clients: Seq[Client[P, Callback]] = config.address.map(address => createClient(address))

  private var permutations = permutationFactory(clients)

  override def send(request: P#Request): Callback[P#Response] = {
    config.requestRetry match {
      case NoRetry =>
        permutations.next().head.send(request)
      case BackoffPolicy(_, _, _, maxTries, _) =>
        if (clients.isEmpty) {
          Callback.failed(new SendFailedException(0, new Exception("Empty client list!")))
        } else {
          val retryList = maxTries match {
            case None =>
              permutations.next().take(clients.size)
            case Some(limit) =>
              if (clients.size < limit) {
                // this is very hacky, probably should rewrite PermutationGenerator to better support this behavior
                val list = ArrayBuffer.empty[Client[P, Callback]]
                while (list.size < limit) {
                  list.appendAll(permutations.next().take(limit))
                }
                list.take(limit).toList
              } else {
                permutations.next().take(limit)
              }
          }

          val retryIncident = config.requestRetry.start()

          def go(next: Sender[P, Callback], list: Seq[Sender[P, Callback]]): Callback[P#Response] = {
            next.send(request).recoverWith {
              case throwable =>
                list match {
                  case head :: tail =>
                    retryIncident.nextAttempt() match {
                      case Stop =>
                        Callback.failed(new SendFailedException(retryIncident.attempts + 1, throwable))
                      case RetryNow =>
                        go(head, tail)
                      case RetryIn(time) =>
                        if (time.toMillis == 0) {
                          go(head, tail)
                        } else {
                          Callback.schedule(time)(go(head, tail))
                        }
                    }
                  case Nil =>
                    Callback.failed(new SendFailedException(retryIncident.attempts + 1, throwable))
                }
            }
          }

          go(retryList.head, retryList.tail)
        }
    }

  }

  override def disconnect(): Unit = {
    clients.foreach(_.disconnect())
  }

  // this is fine on clients, but not sure it is useful on the load balancer; if this is just used for tests, maybe
  // it should not be exposed
  override def connectionStatus: Callback[ConnectionStatus] = {
    Callback.sequence(clients.map(_.connectionStatus)).map { seq =>
      seq.head match {
        case Success(connectStatus) =>
          if (seq.forall(_ == seq.head)) {
            connectStatus
          } else {
            ConnectionStatus.Mixed
          }
        case Failure(exception) =>
          ConnectionStatus.Mixed
      }
    }
  }

  override def address() = throw new NotImplementedError("Load balancer does not have an address")

  override def update(addresses: Seq[InetSocketAddress]): Unit = {
    val newSetup      = addresses.groupBy(a => a).mapValues(_.size)
    val oldClientsMap = clients.groupBy(_.address())

    val newClientsMap = newSetup.foldLeft(Map.empty[InetSocketAddress, Seq[Client[P, Callback]]]) {
      case (aggregation, (address, newCount)) =>
        val newClients = oldClientsMap.get(address) match {
          case None =>
            debug(s"[$address|$newCount] Adding $newCount client(s)")
            (1 to newCount).map(_ => createClient(address))
          case Some(oldClients) =>
            if (oldClients.size == newCount) {
              debug(s"[$address|$newCount] Keeping $newCount client(s)")
              oldClients
            } else if (oldClients.size > newCount) {
              val (keepClients, killClients) = oldClients.splitAt(newCount)
              killClients.foreach(_.disconnect())
              debug(s"[$address|$newCount] Keeping ${keepClients.size} and killing ${killClients.size} client(s)")
              keepClients
            } else {
              val clientsToCreate = newCount - oldClients.size
              debug(s"[$address|$newCount] Adding $clientsToCreate client(s)")
              oldClients ++ (1 to clientsToCreate).map(_ => createClient(address))
            }
        }

        aggregation + (address -> newClients)
    }

    val leftovers = oldClientsMap -- newClientsMap.keys

    leftovers.foreach {
      case (address, deadClients) =>
        debug(s"[$address|0] Killing ${deadClients.size} clients(s)")
    }

    clients = newClientsMap.values.flatten.toSeq
    permutations = permutationFactory(clients)
  }

  private def createClient(address: InetSocketAddress): Client[P, Callback] = {
    val client = clientFactory.createClient(config, address)
    client
  }
}
