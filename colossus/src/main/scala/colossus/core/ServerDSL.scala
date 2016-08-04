package colossus
package core

object ServerDSL {
  type Receive = PartialFunction[Any, Unit]
}
import ServerDSL._
import com.typesafe.config.{Config, ConfigFactory}

/**
 * An instance of this is handed to every new server connection handler
 */
case class ServerContext(server: ServerRef, context: Context)

/**
 * An `Initializer` is used to perform any setup/coordination logic for a
 * [[Server!]] inside a [[WorkerRef Worker]].  Initializers are also used to provide new
 * connections from the server with connection handlers.  An initializer is
 * created per worker, so all actions on a single Initializer are
 * single-threaded.  See [[colossus.core.Server!]] to see how `Initializer` is
 * used when starting servers.
 */
abstract class Initializer(_worker: WorkerRef) {

  implicit val worker = _worker

  /**
   * Given a [[ServerContext]] for a new connection, provide a new connection
   * handler for the connection
   */
  def onConnect: ServerContext => ServerConnectionHandler

  /**
   * Message receive hook.  This is used to handle any messages that are sent
   * using [[ServerRef]] `delegatorBroadcast`.
   */
  def receive: Receive = Map() //empty receive

  /**
   * Shutdown hook that is called when the server is shutting down
   */
  def onShutdown(){}

}

//almost seems like we don't need delegator anymore
class DSLDelegator(server : ServerRef, _worker : WorkerRef, initializer: Initializer) extends Delegator(server, _worker) {


  def acceptNewConnection: Option[ServerConnectionHandler] = {
    Some(initializer.onConnect(ServerContext(server, worker.generateContext)))
  }

  override def handleMessage: Receive = initializer.receive

  override def onShutdown() {
    initializer.onShutdown()
  }

}

//this is mixed in by Server
trait ServerDSL {


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
  def start(name: String, serverConfig: Config = ConfigFactory.load())(initializer: WorkerRef => Initializer)(implicit io: IOSystem): ServerRef = {

    start(name, ServerSettings.load(name, serverConfig))(initializer)
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
    val serverSettings = ServerSettings.load(name).copy(port = port)
    start(name, serverSettings)(initializer)
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
  def basic(name: String, serverConfig: Config = ConfigFactory.load())
           (handlerFactory: ServerContext => ServerConnectionHandler)(implicit io: IOSystem): ServerRef = {

    basic(name, ServerSettings.load(name, serverConfig))(handlerFactory)
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

}
