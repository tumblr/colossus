package colossus
package service

import akka.actor.ActorRef
import core.{WorkerItem, WorkerRef}
import scala.concurrent.{ExecutionContext, Future, Promise}
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
class SendFailedException(tries: Int, finalCause: Throwable) extends Exception(s"Failed after ${tries} tries", finalCause)



/**
 * The LoadBalancingClient will evenly distribute requests across a set of
 * clients.  If one client begins failing, the balancer will retry up to
 * numRetries times across the other clients (with each failover hitting
 * different clients to avoid a cascading pileup
 *
 * Note that the balancer will never try the same client twice for a request,
 * so setting maxTries to a very large number will mean that every client will
 * be tried once
 */
class LoadBalancingClient[I,O] (
  worker: WorkerRef,
  generator: InetSocketAddress => ServiceClient[I,O], 
  maxTries: Int = Int.MaxValue,   
  initialClients: Seq[InetSocketAddress] = Nil
) extends LocalClient[I,O] with WorkerItem {


  private val clients = collection.mutable.ArrayBuffer[ServiceClient[I,O]]()

  private var permutations = new PermutationGenerator(clients.toList)

  update(initialClients)

  worker.bind(this)

  //note, this type must be inner to avoid type erasure craziness
  case class Send(request: I, promise: Promise[O])


  private def regeneratePermutations() {
    permutations = new PermutationGenerator(clients.toList)
  }

  def currentClients = clients.toList

    
  private def addClient(address: InetSocketAddress, regen: Boolean): ServiceClient[I,O] = {
    val client = generator(address)
    clients.append(client)
    regeneratePermutations()
    client
  }

  def addClient(address: InetSocketAddress): ServiceClient[I,O] = addClient(address, true)

  def removeClient(client: ServiceClient[I,O]) {
    client.gracefulDisconnect()
    clients.remove(clients.indexOf(client))
    regeneratePermutations()
  }

  def removeClient(address: InetSocketAddress) {
    val client = clients.find{_.config.address == address}.getOrElse(
      throw new LoadBalancingClientException(s"Tried to remove non-existant client: $address")
    )
    removeClient(client)
    regeneratePermutations()
  }

  /**
   * Updates the client list, creating connections for new addresses not in the
   * existing list and closing connections not in the new list
   */
  def update(addresses: Seq[InetSocketAddress]) {
    val toRemove = clients.filter{client => !addresses.contains(client.config.address)}
    toRemove.foreach(removeClient)
    addresses.foreach{address => 
      if (!clients.exists{_.config.address == address}) {
        addClient(address,false)
      }
    }
    regeneratePermutations()
  }
      

  def send(request: I): Callback[O] = {
    val retryList =  permutations.next().take(maxTries)
    def go(next: LocalClient[I,O], list: List[LocalClient[I, O]]): Callback[O] = next.send(request).recoverWith{
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

  /**
   * Returns a shared version of the client that can be used across threads,
   * inside futures, etc.
   *
   * TODO: There is probably room to generalize some of this plumbing
   */
  def shared(implicit ex: ExecutionContext): SharedClient[I,O] = binding.map{binding => new SharedClient[I,O] {
    
    def send(request: I): Future[O] = {
      val promise = Promise[O]()
      binding.send(Send(request, promise))
      promise.future      
    }
  }}.getOrElse(throw new LoadBalancingClientException("Attempted to get shared interface from unbound load balancer"))

}

