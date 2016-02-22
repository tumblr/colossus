package colossus
package core

object ServerDSL {
  type Receive = PartialFunction[Any, Unit]
}
import ServerDSL._

/**
 * An instance of this is handed to every new server connection handler
 */
case class ServerContext(server: ServerRef, context: Context)

abstract class Initializer(_worker: WorkerRef) {

  implicit val worker = _worker

  def onConnect: ServerContext => ServerConnectionHandler

  //delegator message handling
  def receive: Receive = Map() //empty receive

}

//almost seems like we don't need delegator anymore
class DSLDelegator(server : ServerRef, _worker : WorkerRef, initializer: Initializer) extends Delegator(server, _worker) {


  def acceptNewConnection: Option[ServerConnectionHandler] = {
    Some(initializer.onConnect(ServerContext(server, worker.generateContext)))
  }

  override def handleMessage: Receive = initializer.receive

}

//this is mixed in by Server
trait ServerDSL {

  def start(name: String, settings: ServerSettings)(initializer: WorkerRef => Initializer)(implicit io: IOSystem) : ServerRef = {
    val serverConfig = ServerConfig(
      name = name,
      settings = settings,
      delegatorFactory = (s,w) => new DSLDelegator(s,w, initializer(w))
    )
    Server(serverConfig)

  }

  def start(name: String, port: Int)(initializer: WorkerRef => Initializer)(implicit io: IOSystem): ServerRef = start(name, ServerSettings(port))(initializer)


  def basic(name: String, port: Int)(handlerFactory: ServerContext => ServerConnectionHandler)(implicit io: IOSystem): ServerRef = {
    start(name, port){worker => new Initializer(worker){
      def onConnect = handlerFactory
    }}
  }

  def basic(name: String, settings: ServerSettings)(handlerFactory: ServerContext => ServerConnectionHandler)(implicit io: IOSystem): ServerRef = {
    start(name, settings)(worker => new Initializer(worker) {
      def onConnect = handlerFactory
    })
  }

}

