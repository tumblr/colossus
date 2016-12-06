package colossus
package core

import akka.event.Logging
import java.nio.channels.SocketChannel

import scala.util.Try

/**
 * A Delegator is in charge of creating new ConnectionHandler’s for each new connection.
 * Delegators live inside workers and run as part of the worker’s event loop
 *
 * Delegators are the liaison between the outside world and your application's connection handling logic.
 *
 * They can hold state, and pass that state to newly created ConnectionHandlers.
 *
 * They can receive messages from the outside world, and thus be notified of changes in their environment by way of the handleMessage.
 *
 * This is triggered by utilizing the ServerRef's delegatorBroadcast function.
 *
 *  @param server reference to the server
 *  @param worker reference to the worker this delegator belongs to
 */
abstract class Delegator(val server: ServerRef,  _worker: WorkerRef) {

  //this is so users can do import context.worker
  implicit val worker = _worker

  val log = Logging(worker.system.actorSystem, s"${server.name}-delegator-${worker.id}")

  implicit val executor = server.system.actorSystem.dispatcher


  /**
   * Function which determines whether or not to accept a connection.
   * @return
   */
  protected def acceptNewConnection : Option[ServerConnectionHandler]

  /**
   * This is the function called by the Workers to create an Option[ConnectionHandler]
   * @return
   */
  def createConnectionHandler: Option[ServerConnectionHandler] = {
    val t: Try[Option[ServerConnectionHandler]] = Try(acceptNewConnection)
    t.recover{ case e =>
      log.error(e, s"error while creating connectionHandler in worker ${worker.id}")
      None
    }.get
  }

  /**
   * Shutdown hook.  Called when a Worker stop or a server is unregistered.
   */
  def onShutdown(){}

  /**
   * This allows a Delegator to receive messages which are sent by way of ServerRef.delegatorBroadcast(msg : Any)
   * @return
   */
  def handleMessage: PartialFunction[Any, Unit] = PartialFunction.empty
}

object Delegator {
  type Factory = (ServerRef, WorkerRef) => Delegator

}
