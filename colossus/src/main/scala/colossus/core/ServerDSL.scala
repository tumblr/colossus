package colossus
package core

object ServerDSL {
  type Receive = PartialFunction[Any, Unit]
}
import ServerDSL._
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

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

  val ConfigRoot = "colossus.server"

  /**
    * Create a new Server
    *
    * A Config object will be created via {{{ConfigFactory.load()}}}
    *
    * @param name The name of this Server
    * @param serverConfig Config object in the shape of `colossus.server`
    * @param initializer
    * @param io
    * @return
    */
  def start(name: String, serverConfig: Config = loadConfig)(initializer: WorkerRef => Initializer)(implicit io: IOSystem): ServerRef = {

    start(name, ServerSettings.extract(serverConfig))(initializer)
  }

  /**
    * Convenience function for starting a server using the specified name and port.  This will be overlaid on top of the default
    * "colossus.server" configuration path.
    *
    * @param name Name of this Server.
    * @param port Port to run on
    * @param initializer
    * @param io
    * @return
    */
  def start(name: String, port: Int)(initializer: WorkerRef => Initializer)(implicit io: IOSystem): ServerRef = {
    val serverConfig = loadConfig
    val serverSettings = ServerSettings.extract(serverConfig)
    start(name, serverSettings.copy(port = port))(initializer)
  }

  /**
    * Create a new Server.
    *
    * @param name  Name of this Server.
    * @param settings Settings for this Server.
    * @param initializer
    * @param io
    * @return
    */
  def start(name: String, settings: ServerSettings)(initializer: WorkerRef => Initializer)(implicit io: IOSystem): ServerRef = {
    val serverConfig = ServerConfig(
      name = name,
      settings = settings, delegatorFactory = (s, w) => new DSLDelegator(s, w, initializer(w))
    )
    Server(serverConfig)

  }

  /**
    * Create a new Server
    *
    * A Config object will be created via {{{ConfigFactory.load()}}}
    *
    * @param name Name of this Server
    * @param serverConfig Config object in the shape of `colossus.server`
    * @param handlerFactory
    * @param io
    * @return
    */
  def basic(name: String, serverConfig: Config = loadConfig)
           (handlerFactory: ServerContext => ServerConnectionHandler)(implicit io: IOSystem): ServerRef = {

    basic(name, ServerSettings.extract(serverConfig))(handlerFactory)
  }

  /**
    * Convenience function for starting a server using the specified name and port.  This will be overlaid on top of the default
    * `colossus.server` configuration path.
    *
    * @param name Name of this Server.
    * @param port Port to run on
    * @param handlerFactory
    * @param io
    * @return
    */
  def basic(name: String, port: Int)(handlerFactory: ServerContext => ServerConnectionHandler)(implicit io: IOSystem): ServerRef = {
    start(name, port) { worker => new Initializer(worker) {
      def onConnect = handlerFactory
    }}
  }

  /**
    * Create a new Server.
    *
    * @param name Name of this Server.
    * @param settings Settings for this Server.
    * @param handlerFactory
    * @param io
    * @return
    */
  def basic(name: String, settings: ServerSettings)
           (handlerFactory: ServerContext => ServerConnectionHandler)(implicit io: IOSystem): ServerRef = {
    start(name, settings)(worker => new Initializer(worker) {
      def onConnect = handlerFactory
    })
  }

  private def loadConfig = ConfigFactory.load().getConfig(ConfigRoot)
}