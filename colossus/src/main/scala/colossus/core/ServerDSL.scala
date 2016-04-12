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
    * Create a new Server, using only the defaults provided by the corresponding "colossus.server" config path.
    * A Config object will be created via {{{ConfigFactory.load()}}}
    *
    * @param initializer
    * @param io
    * @return
    */
  def start()(initializer: WorkerRef => Initializer)(implicit io: IOSystem) : ServerRef = {
    start(ConfigRoot)(initializer)
  }

  /**
    * Create a new Server using the supplied configPath.  This configPath will be overlaid on top of the default "colossus.server"
    * config path.
    * A Config object will be created via {{{ConfigFactory.load()}}}
    *
    * @param configPath The path to the configuration.
    * @param initializer
    * @param io
    * @return
    */
  def start(configPath : String)(initializer: WorkerRef => Initializer)(implicit io: IOSystem) : ServerRef = {
    start(configPath, ConfigFactory.load())(initializer)
  }

  /**
    * Create a new Server by loading its config from the specified configPath.
    * This configPath will be overlaid on top of the default "colossus.server" config path.
    *
    * @param configPath The path to the configuration.
    * @param config The Config source to query
    * @param initializer
    * @param io
    * @return
    */
  def start(configPath : String, config : Config)(initializer: WorkerRef => Initializer)(implicit io: IOSystem) : ServerRef = {
    val userPathObject = config.getObject(configPath)
    val serverConfig = userPathObject.withFallback(config.getObject(ConfigRoot)).toConfig
    val name = serverConfig.getString("name")
    start(name, ServerSettings.extract(serverConfig))(initializer)
  }

  /**
    * Convenience function for starting a server using the specified name and port.  This will be overlaid on top of the default
    * "colossus.server" configuration path.
    *
    * @param name Name of this Server. Name is also the MetricAddress relative to the containing IOSystem's MetricNamespace
    * @param port Port to run on
    * @param initializer
    * @param io
    * @return
    */
  def start(name: String, port: Int)(initializer: WorkerRef => Initializer)(implicit io: IOSystem): ServerRef = {

    val user =
      s"""
        |user-settings{
        |  name : "$name"
        |  port : $port
        |}
      """.stripMargin
    val userConfig = ConfigFactory.parseString(user)

    start("user-settings",userConfig.withFallback(ConfigFactory.load()))(initializer)
  }

  /**
    * Create a new Server.
    *
    * @param name Name of this Server. Name is also the MetricAddress relative to the containing IOSystem's MetricNamespace.
    * @param settings Settings for this Server.
    * @param initializer
    * @param io
    * @return
    */
  def start(name: String, settings: ServerSettings)(initializer: WorkerRef => Initializer)(implicit io: IOSystem) : ServerRef = {
    val serverConfig = ServerConfig(
      name = name,
      settings = settings, delegatorFactory = (s,w) => new DSLDelegator(s,w, initializer(w))
    )
    Server(serverConfig)

  }

  /**
    * Create a new Server, using only the defaults provided by the corresponding "colossus.server" config path.
    * A Config object will be created via {{{ConfigFactory.load()}}}
    *
    * @param handlerFactory
    * @param io
    * @return
    */
  def basic()(handlerFactory: ServerContext => ServerConnectionHandler)(implicit io: IOSystem) : ServerRef = {
    basic(ConfigRoot)(handlerFactory)
  }

  /**
    * Create a new Server using the supplied configPath.  This configPath will be overlaid on top of the default "colossus.server"
    * config path.
    * A Config object will be created via {{{ConfigFactory.load()}}}
    *
    * @param configPath The path to the configuration.
    * @param handlerFactory
    * @param io
    * @return
    */
  def basic(configPath : String)(handlerFactory: ServerContext => ServerConnectionHandler)(implicit io: IOSystem) : ServerRef = {
    basic(configPath, ConfigFactory.load())(handlerFactory)
  }

  /**
    * Create a new Server by loading its config from the specified configPath.
    * This configPath will be overlaid on top of the default "colossus.server" config path.
    *
    * @param configPath The path to the configuration.
    * @param config The Config source to query
    * @param handlerFactory
    * @param io
    * @return
    */
  def basic(configPath : String, config : Config)(handlerFactory: ServerContext => ServerConnectionHandler)(implicit io: IOSystem) : ServerRef = {
    val userPathObject = config.getObject(configPath)
    val serverConfig = userPathObject.withFallback(config.getObject(ConfigRoot)).toConfig
    val name = serverConfig.getString("name")
    basic(name, ServerSettings.extract(serverConfig))(handlerFactory)
  }

  /**
    * Convenience function for starting a server using the specified name and port.  This will be overlaid on top of the default
    * "colossus.server" configuration path.
    *
    * @param name Name of this Server. Name is also the MetricAddress relative to the containing IOSystem's MetricNamespace
    * @param port Port to run on
    * @param handlerFactory
    * @param io
    * @return
    */
  def basic(name: String, port: Int)(handlerFactory: ServerContext => ServerConnectionHandler)(implicit io: IOSystem): ServerRef = {
    start(name, port){worker => new Initializer(worker){
      def onConnect = handlerFactory
    }}
  }

  /**
    * Create a new Server.
    *
    * @param name Name of this Server. Name is also the MetricAddress relative to the containing IOSystem's MetricNamespace
    * @param settings Settings for this Server.
    * @param handlerFactory
    * @param io
    * @return
    */
  def basic(name: String, settings: ServerSettings)(handlerFactory: ServerContext => ServerConnectionHandler)(implicit io: IOSystem): ServerRef = {
    start(name, settings)(worker => new Initializer(worker) {
      def onConnect = handlerFactory
    })
  }
}

