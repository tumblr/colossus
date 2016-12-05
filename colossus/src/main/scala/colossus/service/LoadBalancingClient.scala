package colossus
package service

import akka.actor.ActorRef
import core.{WorkerItem, WorkerRef}
import scala.concurrent.Promise
import scala.reflect.ClassTag

import java.net.InetSocketAddress

/**
 * The PermutationGenerator creates permutations such that consecutive calls
 * are guaranteed to cycle though all items as the first element.
 *
 * This currently doesn't iterate through every possible permutation, but it
 * does evenly distribute 1st and 2nd tries...needs some more work
 */
class PermutationGenerator[T : ClassTag](val seedlist: Seq[T]) extends Iterator[List[T]] {
  private val items:Array[T] = seedlist.toArray

  private var swapIndex = 1

  private val cycleSize = seedlist.size * (seedlist.size - 1)
  private var cycleCount = 0

  def hasNext = true

  private def swap(indexA: Int, indexB: Int) {
    val tmp = items(indexA)
    items(indexA) = items(indexB)
    items(indexB) = tmp
  }

  def next(): List[T] = {
    if (items.size == 1) {
      items.head
    } else {
      swap(0, swapIndex)
      swapIndex += 1
      if (swapIndex == items.size) {
        swapIndex = 1
      }
      cycleCount += 1
      if (items.size > 3) {
        if (cycleCount == cycleSize) {
          cycleCount = 0
          swapIndex += 1
          if (swapIndex == items.size) {
            swapIndex = 1
          }
        }
      }

    }
    items.toList
  }

}

class LoadBalancingClientException(message: String) extends Exception(message)
class SendFailedException(tries: Int, finalCause: Throwable) extends Exception(
  s"Failed after ${tries} tries, error on last try: ${finalCause.getMessage}",
  finalCause
)



/**
 * The LoadBalancingClient will evenly distribute requests across a set of
 * clients.  If one client begins failing, the balancer will retry up to
 * numRetries times across the other clients (with each failover hitting
 * different clients to avoid a cascading pileup
 *
 * Note that the balancer will never try the same client twice for a request,
 * so setting maxTries to a very large number will mean that every client will
 * be tried once
 *
 * TODO: does this need to actually be a WorkerItem anymore?
 */
class LoadBalancingClient[P <: Protocol] (
  worker: WorkerRef,
  generator: InetSocketAddress => Sender[P, Callback],
  maxTries: Int = Int.MaxValue,
  initialClients: Seq[InetSocketAddress] = Nil
) extends WorkerItem(worker.generateContext) with Sender[P, Callback]  {

  worker.bind(_ => this)

  case class Client(address: InetSocketAddress, client: Sender[P, Callback])

  private val clients = collection.mutable.ArrayBuffer[Client]()

  private var permutations = new PermutationGenerator(clients.map{_.client})

  update(initialClients, true)

  private def regeneratePermutations() {
    permutations = new PermutationGenerator(clients.map{_.client})
  }

  private def addClient(address: InetSocketAddress, regen: Boolean): Unit = {
    val client = Client(address, generator(address))
    clients append client
    regeneratePermutations()
  }

  def addClient(address: InetSocketAddress): Unit = addClient(address, true)

  private def removeFilter(filter: Client => Boolean) {
    var i = 0
    while (i < clients.length) {
      if (filter(clients(i))) {
        clients(i).client.disconnect()
        clients.remove(i)
      } else {
        i += 1
      }
    }
  }
  
  def removeClient(address: InetSocketAddress) {
    removeFilter{c =>
      c.address == address
    }
    regeneratePermutations()
  }

  /**
   * Updates the client list, creating connections for new addresses not in the
   * existing list and closing connections not in the new list
   */
  def update(addresses: Seq[InetSocketAddress], allowDuplicates: Boolean = false) {
    removeFilter(c => !addresses.contains(c.address))
    addresses.foreach{address =>
      if (! clients.exists{_.address == address} || allowDuplicates) {
        addClient(address,false)
      }
    }
    regeneratePermutations()
  }

  def disconnect() {
    clients.foreach{_.client.disconnect()}
    clients.clear()
  }


  def send(request: P#Input): Callback[P#Output] = {
    val retryList =  permutations.next().take(maxTries)
    def go(next: Sender[P, Callback], list: List[Sender[P, Callback]]): Callback[P#Output] = next.send(request).recoverWith{
      case err => list match {
        case head :: tail => go(head, tail)
        case Nil => Callback.failed(new SendFailedException(retryList.size, err))
      }
    }
    if (retryList.isEmpty) {
      Callback.failed(new SendFailedException(retryList.size, new Exception("Empty client list!")))
    } else {
      go(retryList.head, retryList.tail)
    }
  }

  override def receivedMessage(message: Any, sender: ActorRef) {
  }

}

