package colossus
package core

import akka.actor._
import akka.agent.Agent
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import java.net.{InetSocketAddress, ServerSocket}
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}
import metrics._
import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.Future





/** Contains values for configuring how a Server operates
 *
 * These are all lower-level configuration settings that are for the most part
 * not concerned with a server's application behavior
 *
 * The low/high watermark percentages are used to help mitigate connection
 * overload.  When a server hits the high watermark percentage of live
 * connections, it will change the idle timeout from `maxIdleTime` to
 * `highWaterMaxIdleTime` in an attempt to more aggressively close idle
 * connections.  This will continue until the percentage drops below
 * `lowWatermarkPercentage`.  This can be totally disabled by just setting both
 * watermarks to 1.
 *
 * @param port Port on which this Server will accept connections
 * @param maxConnections Max number of simultaneous live connections
 * @param lowWatermarkPercentage Percentage of live/max connections which represent a normal state
 * @param highWatermarkPercentage Percentage of live/max connections which represent a high water state.
 * @param maxIdleTime Maximum idle time for connections when in non high water conditions
 * @param highWaterMaxIdleTime Max idle time for connections when in high water conditions.
 * @param tcpBacklogSize Set the max number of simultaneous connections awaiting accepting, or None for NIO default
 *
 * @param bindingRetry A [[colossus.core.RetryPolicy]] describing how to retry
 * binding to the port if the first attempt fails.  By default it will keep
 * retrying forever.
 *
 * @param delegatorCreationPolicy A [[colossus.core.WaitPolicy]] describing how
 * to handle delegator startup.  Since a Server waits for a signal from the
 * [[colossus.IOSystem]] that every worker has properly initialized a
 * [[colossus.core.Delegator]], this determines how long to wait before the
 * initialization is considered a failure and whether to retry the
 * initialization.
 *
 * @param shutdownTimeout Once a Server begins to shutdown, it will signal a
 * request to every open connection.  This determines how long it will wait for
 * every connection to self-terminate before it forcibly closes them and
 * completes the shutdown.
 */
case class ServerSettings(
  port: Int,
  slowStart: ConnectionLimiterConfig = ConnectionLimiterConfig.NoLimiting,
  maxConnections: Int = 1000,
  maxIdleTime: Duration = Duration.Inf,
  lowWatermarkPercentage: Double = 0.75,
  highWatermarkPercentage: Double = 0.85,
  highWaterMaxIdleTime : FiniteDuration = 100.milliseconds,
  tcpBacklogSize: Option[Int] = None,
  bindingRetry : RetryPolicy = BackoffPolicy(100.milliseconds, BackoffMultiplier.Exponential(1.second)),
  delegatorCreationPolicy : WaitPolicy = WaitPolicy(500.milliseconds, BackoffPolicy(50.milliseconds, BackoffMultiplier.Constant)),
  shutdownTimeout: FiniteDuration = 100.milliseconds
) {
  def lowWatermark = lowWatermarkPercentage * maxConnections
  def highWatermark = highWatermarkPercentage * maxConnections
}

object ServerSettings {
  val ConfigRoot = "colossus.server"

  def extract(config : Config) : ServerSettings = {
    import colossus.metrics.ConfigHelpers._

    val bindingRetry = RetryPolicy.fromConfig(config.getConfig("binding-retry"))
    val delegatorCreationPolicy = WaitPolicy.fromConfig(config.getConfig("delegator-creation-policy"))

    ServerSettings (
      port                    = config.getInt("port"),
      maxConnections          = config.getInt("max-connections"),
      slowStart               = ConnectionLimiterConfig.fromConfig(config.getConfig("slow-start")),
      maxIdleTime             = config.getScalaDuration("max-idle-time"),
      lowWatermarkPercentage  = config.getDouble("low-watermark-percentage"),
      highWatermarkPercentage = config.getDouble("high-watermark-percentage"),
      highWaterMaxIdleTime    = config.getFiniteDuration("highwater-max-idle-time"),
      tcpBacklogSize          = config.getIntOption("tcp-backlog-size"),
      bindingRetry            = bindingRetry,
      delegatorCreationPolicy = delegatorCreationPolicy,
      shutdownTimeout         = config.getFiniteDuration("shutdown-timeout")
    )
  }

  def load(name: String, config: Config = ConfigFactory.load()): ServerSettings = {
    val nameRoot = ConfigRoot + "." + name
    val resolved = if (config.hasPath(nameRoot)) {
      config.getConfig(nameRoot).withFallback(config.getConfig(ConfigRoot))
    } else {
      config.getConfig(ConfigRoot)
    }
    extract(resolved)

  }
}

/** Configuration used to specify a Server's application-level behavior
 *
 *  As opposed to ServerSettings which contains just lower-level config,
 *  ServiceConfig contains higher-level settings.  Ideally, abstraction layers
 *  on top of the core layer should provide an API that allows users to create
 *  a specialized server, where the delegator factory is provided by the API
 *  and the name and settings by the user.
 *
 * @param name Name of the Server, all reported metrics are prefixed using this name
 * @param delegatorFactory Factory to generate [[colossus.core.Delegator]]s for each Worker
 * @param settings lower-level server configuration settings
 */
case class ServerConfig(
  name: MetricAddress, //possibly move this into settings as well, needs more experimentation
  delegatorFactory: Delegator.Factory,
  settings : ServerSettings
)

/**
 * A `ServerRef` is a handle to a created server.  It can be used to get basic
 * information about the state of the server as well as send operational
 * commands to it.
 *
 * @param config The ServerConfig used to create this Server
 * @param server The ActorRef of the Server
 * @param system The IOSystem to which this Server belongs
 * @param serverStateAgent The current state of the Server.
 */
case class ServerRef private[colossus] (config: ServerConfig, server: ActorRef, system: IOSystem, private val serverStateAgent : Agent[ServerState]) {
  def name = config.name

  def serverState = serverStateAgent.get()

  val namespace : MetricNamespace = system.namespace / name

  def maxIdleTime = {
    if(serverStateAgent().connectionVolumeState == ConnectionVolumeState.HighWater) {
      config.settings.highWaterMaxIdleTime
    } else {
      config.settings.maxIdleTime
    }
  }

  def info(): Future[Server.ServerInfo] = {
    implicit val timeout = Timeout(1.second)
    (server ? Server.GetInfo).mapTo[Server.ServerInfo]
  }

  /**
   * Broadcast a message to a all of the [[Delegator]]s of this server.
   *
   * @param message
   * @param sender
   * @return
   */
  def delegatorBroadcast(message: Any)(implicit sender: ActorRef = ActorRef.noSender) {
    server.!(Server.DelegatorBroadcast(message))(sender)
  }

  /**
   * Gracefully shutdown this server.  This will cause the server to
   * immediately begin refusing connections, but attempt to allow existing
   * connections to close on their own.  The `shutdownRequest` method will be
   * called on every `ServerConnectionHandler` associated with this server.
   * `shutdownTimeout` in `ServerSettings` controls how long the server will
   * wait before it force-closes all connections and shuts down.  The server
   * actor will remain alive until it is fully shutdown.
   */
  def shutdown() {
    server ! Server.Shutdown
  }

  /**
   * Immediately kill the server and all corresponding open connections.
   */
  def die() {
    server ! PoisonPill
  }
}

/**
 * Represents the current state of a Server.
 *
 * @param connectionVolumeState Represents if the a Server's connections are normal or in highwater
 * @param serverStatus Represents the Server's current status
 */
case class ServerState(connectionVolumeState : ConnectionVolumeState, serverStatus : ServerStatus)

/**
 * ConnectionVolumeState indicates whether or not if the Server is operating
 * with a normal workload, which is represented by the current ratio of used /
 * available connections being beneath the
 * ServerSettings.highWatermarkPercentage amount.  Once this ratio is breached,
 * the state will be changed to the HighWater state.
 *
 * In this state, the server will more aggressively timeout and close idle
 * connections.  Effectively what this means is that when the Server polls its
 * connections in the Highwater state, any connection that is seen as idle for
 * longer than ServerSettings.highWaterMaxIdleTime will be reaped.  It does this
 * in an effort to free up space.
 *
 * The Server will return back to its normal state when the connection ratio
 * recedes past the lowWater mark.
 */
sealed trait ConnectionVolumeState

object ConnectionVolumeState {
  case object Normal extends ConnectionVolumeState
  case object HighWater extends ConnectionVolumeState
}

private[colossus] class Server(io: IOSystem, serverConfig: ServerConfig,
                               stateAgent : Agent[ServerState]) extends Actor with ActorLogging with Stash {
  import Server._
  import context.dispatcher
  import serverConfig._
  import ServerStatus._
  import ConnectionVolumeState._

  val metricSystem = io.metrics
  val selector: Selector = Selector.open()
  val ssc = ServerSocketChannel.open()
  ssc.configureBlocking(false)
  val ss: ServerSocket = ssc.socket()
  val address = new InetSocketAddress(settings.port)
  ssc.register( selector, SelectionKey.OP_ACCEPT )

  val me = ServerRef(serverConfig, self, io, stateAgent)

  val connectionLimiter = ConnectionLimiter(settings.maxConnections, settings.slowStart)

  //initialize metrics
  implicit val ns = me.namespace
  val connections   = Counter("connections", "server-connections")
  val refused       = Rate("refused_connections", "server-refused-connections")
  val connects      = Rate("connects", "server-connects")
  val closed        = Rate("closed", "server-closed")
  val highwaters    = Rate("highwaters", "server-highwaters")

  private var openConnections = 0

  def closeConnection(cause: RootDisconnectCause) {
    openConnections -= 1
    connections.decrement()
    closed.hit(tags = Map("cause" -> cause.tagString))
    updateServerConnectionState()
  }

  def start() = {
    //setup the server
    try {
      ss.bind(address, settings.tcpBacklogSize.getOrElse(0))
      log.info(s"name: Bound to port ${settings.port}")
      true
    } catch {
      case t: Throwable => {
        log.error(t, s"bind failed: ${t.getMessage}, retrying")
        false
      }
    }
  }

  private def changeState(receive : Receive, status : ServerStatus) {
    log.debug(s"changing state to $status")
    context.become(receive)
    updateServerStatus(status)
  }

  def alwaysHandle : Receive = handleShutdown orElse handleStatus

  def receive = waitForWorkers


  def waitForWorkers: Receive = alwaysHandle orElse {
    case WorkerManager.WorkersReady(workers) => {
      log.debug(s"workers are ready, attempting to bind to port ${settings.port}")
      changeState(binding(workers), ServerStatus.Binding)
      self ! RetryBind(None)
      unstashAll()
    }
    case WorkerManager.RegistrationFailed => {
      log.error(s"Could not register with the IOSystem.  Taking PoisonPill")
      self ! PoisonPill
    }
    case d: DelegatorBroadcast => stash()
    case Shutdown => {
      self ! PoisonPill
    }
  }


  def binding(router: ActorRef): Receive = alwaysHandle orElse {
    case DelegatorBroadcast(message) => router ! akka.routing.Broadcast(Worker.DelegatorMessage(me, message))
    case RetryBind(incidentOpt) => {
      if (start()) {
        changeState(accepting(router), Bound)
        connectionLimiter.begin()
        self ! Select
      } else {
        val incident = incidentOpt.getOrElse(settings.bindingRetry.start())
        val message = s"Failed to bind to port ${settings.port} after ${incident.attempts} attempts."
        incident.nextAttempt() match {
          case RetryAttempt.Stop => {
            log.error( s"$message Shutting Down!")
            self ! PoisonPill
          }
          case RetryAttempt.RetryNow => {
            log.error(s"$message Retrying")
            self ! RetryBind(Some(incident))
          }
          case RetryAttempt.RetryIn(time) => {
            log.error(s"$message Retrying in $time.")
            context.system.scheduler.scheduleOnce(time, self, RetryBind(Some(incident)))
          }
        }
      }
    }
    case Shutdown => {
      //no need to enter the shuttingDown state since we know there's no open connections
      self ! PoisonPill
    }
  }

  def accepting(router: ActorRef): Receive = alwaysHandle orElse {
    case Select => {
      selectLoop(router)
      self ! Select
    }
    case ConnectionClosed(id, cause) => closeConnection(cause)
    case ConnectionRefused(sc, attempt) => if (attempt >= Server.MaxConnectionRegisterAttempts) {
      log.error(s"Failed to register new connection $attempt times, closing")
      sc.close()
      self ! ConnectionClosed(0, DisconnectCause.Error(new Server.MaxConnectionRegisterException))
    } else {
      router ! Worker.NewConnection(sc, attempt + 1)
    }
    case DelegatorBroadcast(message) => router ! akka.routing.Broadcast(Worker.DelegatorMessage(me, message))
    case Shutdown => {
      //initiate the shutdown sequence.  We broadcast a shutdown request to all
      //of our open connections, and then wait for them to close or timeout and
      //force-close them.  We also stop accepting new connections
      ss.close()
      io.workerManager ! WorkerManager.ServerShutdownRequest(me)
      context.system.scheduler.scheduleOnce(shutdownCheckFrequency, self, ShutdownCheck)
      changeState(shuttingDown(System.currentTimeMillis), ServerStatus.ShuttingDown)
    }
  }

  def handleShutdown: Receive = {
    case Terminated(w) => {
      //if the worker manager is terminated, that's it for this server
      self ! PoisonPill
    }
  }

  def shuttingDown(startTime: Long): Receive = alwaysHandle orElse {
    case ConnectionClosed(id, cause) => closeConnection(cause)
    case ShutdownCheck => if (System.currentTimeMillis - startTime > settings.shutdownTimeout.toMillis) {
      log.warning(s"Shutdown timeout of ${settings.shutdownTimeout} reached, terminating ${openConnections} connections and shutting down")
      self ! PoisonPill
    } else if (openConnections == 0) {
      self ! PoisonPill
    } else {
      log.info(s"waiting for $openConnections connections to close")
      context.system.scheduler.scheduleOnce(Server.shutdownCheckFrequency, self, ShutdownCheck)
    }
  }


  def handleStatus : Receive = {
    case GetInfo => sender ! ServerInfo(openConnections, serverStatus)
  }

  def selectLoop(router: ActorRef) {
    selector.select(5)
    val selectedKeys = selector.selectedKeys()
    val it = selectedKeys.iterator()

    while (it.hasNext) {
      val key : SelectionKey = it.next
      if (!key.isValid) {
        log.error("KEY IS INVALID")
        it.remove()
      } else if (key.isAcceptable) {
        // Accept the new connection
        try {
          val ssc : ServerSocketChannel = key.channel.asInstanceOf[ServerSocketChannel] //oh, java
          val sc: SocketChannel = ssc.accept()
          connects.hit()
          if (openConnections < connectionLimiter.limit) {
            openConnections += 1
            connections.increment()
            sc.configureBlocking(false)
            sc.socket.setTcpNoDelay(true)
            router ! Worker.NewConnection(sc)
            updateServerConnectionState()
          } else {
            sc.close()
            refused.hit()
          }
        } catch {
          case c: java.nio.channels.NotYetBoundException => log.error(c, "Attempted to accept before bound!?")
          case error: Throwable => log.error(error, s"Error accepting connection: ${error.getClass.getName} - ${error.getMessage}")
        }
        it.remove()
      }
    }

  }

  override def preStart() {
    super.preStart()
    context.watch(io.workerManager)
    io.workerManager ! WorkerManager.RegisterServer(me)
    log.info("spinning up server")
  }

  override def postStop() {
    //cleanup
    selector.keys.foreach{_.channel.close}
    selector.close()
    ss.close()
    log.info("SERVER PEACE OUT")
    updateServerStatus(ServerStatus.Dead)
  }

  private def updateServerConnectionState() {
    val connectionState = determineConnectionStateChange(openConnections, settings.lowWatermark, settings.highWatermark, stateAgent().connectionVolumeState)
    connectionState.foreach {
      x => stateAgent.send(stateAgent().copy(connectionVolumeState = x))
        highwaters.hit()
    }
  }

  private def updateServerStatus(serverStatus : ServerStatus) {
    stateAgent.send(stateAgent().copy(serverStatus = serverStatus))
  }

  private def serverStatus: ServerStatus = stateAgent().serverStatus

  private def determineConnectionStateChange(currentCount : Int, lowCount : Double, highCount : Double, previousState : ConnectionVolumeState) : Option[ConnectionVolumeState] = {

    if(currentCount <= lowCount && previousState != Normal){
      Some(Normal)
    }else if(currentCount >= highCount && previousState != HighWater){
      Some(HighWater)
    }else { //if between water marks, or the states are identical
      None
    }
  }
}

/**
 * Represents the startup status of the server.
 *
 *  - `Initializing` : The server was just started and is registering with the IOSystem
 *  - `Binding` : The server is registered and in the process of binding to its port
 *  - `Bound` : The server is actively listening on the port and accepting connections
 *  - `ShuttingDown` : The server is shutting down.  It is no longer accepting new connections and waiting for existing connections to close
 *  - `Dead` : The server is fully shutdown
 *
 */
sealed trait ServerStatus
object ServerStatus {
  case object Initializing  extends ServerStatus
  case object Binding       extends ServerStatus
  case object Bound         extends ServerStatus
  case object ShuttingDown  extends ServerStatus
  case object Dead          extends ServerStatus
}

/**
 * The entry point for starting a Server
 *
 */
object Server extends ServerDSL {

  private[core] val MaxConnectionRegisterAttempts = 3
  class MaxConnectionRegisterException extends Exception("Maximum number of connection register attempts reached")

  private[core] def shutdownCheckFrequency = 50.milliseconds

  private[core] case object Select
  private[core] case object ShutdownCheck
  private[core] case class RetryBind(retry: Option[RetryIncident])

  case class ConnectionClosed(id: Long, cause : RootDisconnectCause)

  /** Sent from a worker to the server when the server is not registered with the worker.
   *
   * This generally happens when a worker has just been killed and restarted.  See Server.MaxConnectionRegisterAttempts
   */
  private[core] case class ConnectionRefused(channel: SocketChannel, attempt: Int)

  private[core] sealed trait ServerCommand
  private[core] case object Shutdown extends ServerCommand
  private[core] case class DelegatorBroadcast(message: Any) extends ServerCommand
  private[core] case object GetInfo extends ServerCommand

  case class ServerInfo(openConnections: Int, status: ServerStatus)


  /**
   * Create a server with the ServerConfig
   * @param serverConfig Contains the desired configuration of this Server
    *               It is expected to be in the shape of the "colossus.server" reference configuration, and is primarily used to configure
    *               Server metrics.
   * @param io The IOSystem to which this Server will belong
   * @return ServerRef which encapsulates the created Server
   */
  def apply(serverConfig: ServerConfig)(implicit io: IOSystem): ServerRef = {
    import io.actorSystem.dispatcher
    import ServerStatus._
    val serverStateAgent = Agent(ServerState(ConnectionVolumeState.Normal, Initializing))
    val actor = io.actorSystem.actorOf(Props(classOf[Server], io, serverConfig, serverStateAgent)
                              .withDispatcher("server-dispatcher"), name = s"server-${serverConfig.name.idString}")
    ServerRef(serverConfig, actor, io, serverStateAgent)
  }

}
