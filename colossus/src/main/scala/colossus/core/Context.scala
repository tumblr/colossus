package colossus.core

import akka.actor.{ActorRef, Props}

/**
  * Represents the binding of an item to a worker
  *
  * @param id the id used to reference the worker item
  * @param worker the worker the item is bound to
  */
class Context(val id: Long, val worker: WorkerRef) {

  def !(message: Any)(implicit sender: ActorRef) {
    worker.worker ! WorkerCommand.Message(id, message)
  }

  def unbind() {
    worker.worker ! WorkerCommand.UnbindWorkerItem(id)
  }

  private[colossus] lazy val proxy: ActorRef = {
    _proxyExists = true
    worker.system.actorSystem.actorOf(Props(classOf[WorkerItemProxy], id, worker))
  }

  private var _proxyExists          = false
  private[colossus] def proxyExists = _proxyExists
}

/**
  * An instance of this is handed to every new server connection handler
  */
case class ServerContext(server: ServerRef, context: Context) {
  def name: String = server.name.idString
}

/**
  * An instance of this is handed to every new initializer for a server
  */
case class InitContext(server: ServerRef, worker: WorkerRef)
