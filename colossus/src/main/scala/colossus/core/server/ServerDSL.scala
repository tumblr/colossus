package colossus.core.server

object ServerDSL {
  type Receive = PartialFunction[Any, Unit]
}
import ServerDSL._
import colossus.IOSystem
import colossus.core._
import com.typesafe.config.{Config, ConfigFactory}

/**
  * An `Initializer` is used to perform any setup/coordination logic for a
  * [[Server!]] inside a [[WorkerRef Worker]].  Initializers are also used to provide new
  * connections from the server with connection handlers.  An initializer is
  * created per worker, so all actions on a single Initializer are
  * single-threaded.  See [[colossus.core.Server!]] to see how `Initializer` is
  * used when starting servers.
  */
abstract class Initializer(context: InitContext) {

  implicit val worker = context.worker

  val server = context.server

  /**
    * Given a [[ServerContext]] for a new connection, provide a new connection
    * handler for the connection
    */
  def onConnect: ServerContext => ServerConnectionHandler

  /**
    * Message receive hook.  This is used to handle any messages that are sent
    * using [[ServerRef]] `initializerBroadcast`.
    */
  def receive: Receive = Map() //empty receive

  /**
    * Shutdown hook that is called when the server is shutting down
    */
  def onShutdown() {}

}

object Initializer {
  type Factory = InitContext => Initializer
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
    * @param initializer Generate server initializer from initial runtime context object
    * @param io the Java I/O process
    * @return
    */
  def start(name: String, serverConfig: Config = ConfigFactory.load())(initializer: InitContext => Initializer)(
      implicit io: IOSystem): ServerRef = {

    start(name, ServerSettings.load(name, serverConfig))(initializer)
  }

  /**
    * Convenience function for starting a server using the specified name and port.  This will be overlaid on top of the default
    * "colossus.server" configuration path.
    *
    * @param name Name of this Server.
    * @param port Port to run on
    * @param initializer Generate server initializer from initial runtime context object
    * @param io the Java I/O process
    * @return
    */
  def start(name: String, port: Int)(initializer: InitContext => Initializer)(implicit io: IOSystem): ServerRef = {
    val serverSettings = ServerSettings.load(name).copy(port = port)
    start(name, serverSettings)(initializer)
  }

  /**
    * Create a new Server.
    *
    * @param name  Name of this Server.
    * @param settings Settings for this Server.
    * @param initializer Generate server initializer from initial runtime context object
    * @param io the Java I/O process
    * @return
    */
  def start(name: String, settings: ServerSettings)(initializer: InitContext => Initializer)(
      implicit io: IOSystem): ServerRef = {
    val serverConfig = ServerConfig(
      name = name,
      settings = settings,
      initializerFactory = (ic) => initializer(ic)
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
    * @param io the Java I/O process
    * @return
    */
  def basic(name: String, serverConfig: Config = ConfigFactory.load())(
      handlerFactory: ServerContext => ServerConnectionHandler)(implicit io: IOSystem): ServerRef = {

    basic(name, ServerSettings.load(name, serverConfig))(handlerFactory)
  }

  /**
    * Convenience function for starting a server using the specified name and port.  This will be overlaid on top of the default
    * `colossus.server` configuration path.
    *
    * @param name Name of this Server.
    * @param port Port to run on
    * @param handlerFactory
    * @param io the Java I/O process
    * @return
    */
  def basic(name: String, port: Int)(handlerFactory: ServerContext => ServerConnectionHandler)(
      implicit io: IOSystem): ServerRef = {
    start(name, port) { worker =>
      new Initializer(worker) {
        def onConnect = handlerFactory
      }
    }
  }

  /**
    * Create a new Server.
    *
    * @param name Name of this Server.
    * @param settings Settings for this Server.
    * @param handlerFactory
    * @param io the Java I/O process
    * @return
    */
  def basic(name: String, settings: ServerSettings)(handlerFactory: ServerContext => ServerConnectionHandler)(
      implicit io: IOSystem): ServerRef = {
    start(name, settings)(worker =>
      new Initializer(worker) {
        def onConnect = handlerFactory
    })
  }

}
