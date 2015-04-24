package colossus
package core

import akka.actor._
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
abstract class Delegator(val server: ServerRef, val worker: WorkerRef) {
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
  def createConnectionHandler: Option[ConnectionHandler] = {
    val t: Try[Option[ConnectionHandler]] = Try(acceptNewConnection)
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

  def basic(acceptor: () => ServerConnectionHandler): Factory = {(s,w) => new Delegator(s,w){
    protected def acceptNewConnection = Some(acceptor())
  }}
}

/**
 * Currently unused; but will eventually be for async delegators, if we decide to allow them
 */
sealed trait DelegatorCommand
object DelegatorCommand {
  case class Reject(sock: SocketChannel) extends DelegatorCommand
  case class Register(server: ActorRef, sock: SocketChannel, handler: ConnectionHandler) extends DelegatorCommand

}
