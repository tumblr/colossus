package colossus
package service

import core.WorkerRef

import java.net.InetSocketAddress

/**
 * A ClientPool is a simple container of open connections.  It can receive
 * updates and will open/close connections accordingly.
 *
 * note that config will be copied for each client, replacing only the address
 */
class ServiceClientPool[T <: Sender[_, Callback]](val commonConfig: ClientConfig, worker: WorkerRef, creator: (ClientConfig, WorkerRef) => T) {

  private val clients = collection.mutable.Map[InetSocketAddress, T]()

  private def create(address: InetSocketAddress) = {
    val config = commonConfig.copy(
      address = address
    )
    val client = creator(config, worker)
    client
  }

  /**
   * Connects to any hosts in the address list not yet connected to,
   * disconnects from any hosts not in the address list
   */
  def update(addresses: List[InetSocketAddress]) {
    val added = addresses.filter{a => !clients.contains(a)}
    val removed = clients.keys.filter{a => !addresses.contains(a)}
    added.foreach{address =>
      clients(address) = create(address)
    }
    removed.foreach{address =>
      val client = clients(address)
      client.disconnect()
      clients -= address
    }
  }

  def apply(address: InetSocketAddress) = clients.get(address).getOrElse{
    val c = create(address)
    clients(address) = c
    c
  }

  def get(address: InetSocketAddress) = clients.get(address)


}

