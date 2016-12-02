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
class PermutationGenerator[T : ClassTag](val seedlist: List[T]) extends Iterator[List[T]] {
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
) extends WorkerItem with Sender[P, Callback]  {

  val context = worker.generateContext

  worker.bind(_ => this)

  type Client = Sender[P, Callback]

  private val clients = collection.mutable.Map[InetSocketAddress, Client]()

  private var permutations = new PermutationGenerator(clients.values.toList)

  update(initialClients)

  //note, this type must be inner to avoid type erasure craziness
  case class Send(request: P#Request, promise: Promise[P#Response])


  private def regeneratePermutations() {
    permutations = new PermutationGenerator(clients.values.toList)
  }

  def currentClients = clients.toList


  private def addClient(address: InetSocketAddress, regen: Boolean): Sender[P, Callback] = {
    val client = generator(address)
    clients(address) = client
    regeneratePermutations()
    client
  }

  def addClient(address: InetSocketAddress): Sender[P, Callback] = addClient(address, true)

  def removeClient(address: InetSocketAddress) {
    val client = clients.get(address).getOrElse(
      throw new LoadBalancingClientException(s"Tried to remove non-existant client: $address")
    )
    clients -= address
    client.disconnect()
    regeneratePermutations()
  }

  /**
   * Updates the client list, creating connections for new addresses not in the
   * existing list and closing connections not in the new list
   */
  def update(addresses: Seq[InetSocketAddress]) {
    val toRemove = clients.filter{case (i, c) => !addresses.contains(i)}.keys
    toRemove.foreach(removeClient)
    addresses.foreach{address =>
      if (! (clients contains address)) {
        addClient(address,false)
      }
    }
    regeneratePermutations()
  }

  def disconnect() {
    clients.foreach{case (i,c) => c.disconnect()}
    clients.clear()
  }


  def send(request: P#Request): Callback[P#Response] = {
    val retryList =  permutations.next().take(maxTries)
    def go(next: Sender[P, Callback], list: List[Sender[P, Callback]]): Callback[P#Response] = next.send(request).recoverWith{
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
    message match {
      case Send(request, promise) => send(request).execute(promise.complete)
    }
  }

}

