package colossus.core.server

import akka.actor._
import java.net.{InetSocketAddress, ServerSocket, StandardSocketOptions}
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}
import java.util.concurrent.atomic.AtomicReference

import colossus.IOSystem
import colossus.core._
import colossus.metrics.MetricAddress
import colossus.metrics.collectors.{Counter, Rate}
import colossus.metrics.logging.ColossusLogging

import scala.collection.JavaConverters._
import scala.concurrent.duration._

/** Configuration used to specify a Server's application-level behavior
  *
  *  As opposed to ServerSettings which contains just lower-level config,
  *  ServiceConfig contains higher-level settings.  Ideally, abstraction layers
  *  on top of the core layer should provide an API that allows users to create
  *  a specialized server, where the delegator factory is provided by the API
  *  and the name and settings by the user.
  *
  * @param name Name of the Server, all reported metrics are prefixed using this name
  * @param initializerFactory Factory to generate [[colossus.core.server.Initializer]]s for each Worker
  * @param settings lower-level server configuration settings
  */
case class ServerConfig(
    name: MetricAddress, //possibly move this into settings as well, needs more experimentation
    initializerFactory: Initializer.Factory,
    settings: ServerSettings
)

/**
  * Represents the current state of a Server.
  *
  * @param connectionVolumeState Represents if the a Server's connections are normal or in highwater
  * @param serverStatus Represents the Server's current status
  */
case class ServerState(connectionVolumeState: ConnectionVolumeState, serverStatus: ServerStatus)

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
  case object Normal    extends ConnectionVolumeState
  case object HighWater extends ConnectionVolumeState
}

private[colossus] class Server(io: IOSystem, serverConfig: ServerConfig, stateRef: AtomicReference[ServerState])
    extends Actor
    with ColossusLogging
    with Stash {
  import Server._
  import context.dispatcher
  import serverConfig._
  import ServerStatus._
  import ConnectionVolumeState._

  val metricSystem       = io.metrics
  val selector: Selector = Selector.open()
  val ssc                = ServerSocketChannel.open()
  ssc.configureBlocking(false)

  serverConfig.settings.reuseAddress.foreach { reuseAddr =>
    // wow, mixing Scala Boolean and Java Boolean gives you Any, which pisses off invariant interfaces. Go figure.
    ssc.setOption[java.lang.Boolean](StandardSocketOptions.SO_REUSEADDR, reuseAddr)
  }

  val ss: ServerSocket = ssc.socket()
  val address          = new InetSocketAddress(settings.port)
  ssc.register(selector, SelectionKey.OP_ACCEPT)

  val me = ServerRef(serverConfig, self, io, stateRef)

  val connectionLimiter = ConnectionLimiter(settings.maxConnections, settings.slowStart)

  //initialize metrics
  implicit val ns = me.namespace
  val connections = Counter("connections", "server-connections")
  val refused     = Rate("refused_connections", "server-refused-connections")
  val connects    = Rate("connects", "server-connects")
  val closed      = Rate("closed", "server-closed")
  val highwaters  = Rate("highwaters", "server-highwaters")

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
      info(s"name: Bound to port ${settings.port}")
      true
    } catch {
      case t: Throwable => {
        error(s"bind failed: ${t.getMessage}, retrying", t)
        false
      }
    }
  }

  private def changeState(receive: Receive, status: ServerStatus) {
    debug(s"changing state to $status")
    context.become(receive)
    updateServerStatus(status)
  }

  def alwaysHandle: Receive = handleShutdown orElse handleStatus

  def receive = waitForWorkers

  def waitForWorkers: Receive = alwaysHandle orElse {
    case WorkerManager.WorkersReady(workers) => {
      debug(s"workers are ready, attempting to bind to port ${settings.port}")
      changeState(binding(workers), ServerStatus.Binding)
      self ! RetryBind(None)
      unstashAll()
    }
    case WorkerManager.RegistrationFailed => {
      error(s"Could not register with the IOSystem.  Taking PoisonPill")
      self ! PoisonPill
    }
    case d: InitializerBroadcast => stash()
    case Shutdown => {
      self ! PoisonPill
    }
  }

  def binding(router: ActorRef): Receive = alwaysHandle orElse {
    case InitializerBroadcast(message) => router ! akka.routing.Broadcast(Worker.InitializerMessage(me, message))
    case RetryBind(incidentOpt) => {
      if (start()) {
        changeState(accepting(router), Bound)
        connectionLimiter.begin()
        self ! Select
      } else {
        val incident = incidentOpt.getOrElse(settings.bindingRetry.start())
        val message  = s"Failed to bind to port ${settings.port} after ${incident.attempts} attempts."
        incident.nextAttempt() match {
          case RetryAttempt.Stop => {
            error(s"$message Shutting Down!")
            self ! PoisonPill
          }
          case RetryAttempt.RetryNow => {
            error(s"$message Retrying")
            self ! RetryBind(Some(incident))
          }
          case RetryAttempt.RetryIn(time) => {
            error(s"$message Retrying in $time.")
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
    case ConnectionRefused(sc, attempt) =>
      if (attempt >= Server.MaxConnectionRegisterAttempts) {
        error(s"Failed to register new connection $attempt times, closing")
        sc.close()
        self ! ConnectionClosed(0, DisconnectCause.Error(new Server.MaxConnectionRegisterException))
      } else {
        router ! Worker.NewConnection(sc, attempt + 1)
      }
    case InitializerBroadcast(message) => router ! akka.routing.Broadcast(Worker.InitializerMessage(me, message))
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
    case ShutdownCheck =>
      if (System.currentTimeMillis - startTime > settings.shutdownTimeout.toMillis) {
        warn(
          s"Shutdown timeout of ${settings.shutdownTimeout} reached, terminating ${openConnections} connections and shutting down")
        self ! PoisonPill
      } else if (openConnections == 0) {
        self ! PoisonPill
      } else {
        info(s"waiting for $openConnections connections to close")
        context.system.scheduler.scheduleOnce(Server.shutdownCheckFrequency, self, ShutdownCheck)
      }
  }

  def handleStatus: Receive = {
    case GetInfo => sender ! ServerInfo(openConnections, serverStatus)
  }

  def selectLoop(router: ActorRef) {
    selector.select(5)
    val selectedKeys = selector.selectedKeys()
    val it           = selectedKeys.iterator()

    while (it.hasNext) {
      val key: SelectionKey = it.next
      if (!key.isValid) {
        error("KEY IS INVALID")
        it.remove()
      } else if (key.isAcceptable) {
        // Accept the new connection
        try {
          val ssc: ServerSocketChannel = key.channel.asInstanceOf[ServerSocketChannel] //oh, java
          val sc: SocketChannel        = ssc.accept()
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
          case c: java.nio.channels.NotYetBoundException => error("Attempted to accept before bound!?", c)
          case t: Throwable =>
            error(s"Error accepting connection: ${t.getClass.getName} - ${t.getMessage}", t)
        }
        it.remove()
      }
    }

  }

  override def preStart() {
    super.preStart()
    context.watch(io.workerManager)
    io.workerManager ! WorkerManager.RegisterServer(me)
    info("spinning up server")
  }

  override def postStop() {
    //cleanup
    selector.keys.asScala.foreach { _.channel.close }
    selector.close()
    ss.close()
    info("SERVER PEACE OUT")
    updateServerStatus(ServerStatus.Dead)
  }

  private def updateServerConnectionState() {
    val connectionState = determineConnectionStateChange(openConnections,
                                                         settings.lowWatermark,
                                                         settings.highWatermark,
                                                         stateRef.get.connectionVolumeState)
    connectionState.foreach { x =>
      stateRef.set(stateRef.get.copy(connectionVolumeState = x))
      highwaters.hit()
    }
  }

  private def updateServerStatus(serverStatus: ServerStatus) {
    stateRef.set(stateRef.get.copy(serverStatus = serverStatus))
  }

  private def serverStatus: ServerStatus = stateRef.get.serverStatus

  private def determineConnectionStateChange(currentCount: Int,
                                             lowCount: Double,
                                             highCount: Double,
                                             previousState: ConnectionVolumeState): Option[ConnectionVolumeState] = {

    if (currentCount <= lowCount && previousState != Normal) {
      Some(Normal)
    } else if (currentCount >= highCount && previousState != HighWater) {
      Some(HighWater)
    } else { //if between water marks, or the states are identical
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
  case object Initializing extends ServerStatus
  case object Binding      extends ServerStatus
  case object Bound        extends ServerStatus
  case object ShuttingDown extends ServerStatus
  case object Dead         extends ServerStatus
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

  case class ConnectionClosed(id: Long, cause: RootDisconnectCause)

  /** Sent from a worker to the server when the server is not registered with the worker.
    *
    * This generally happens when a worker has just been killed and restarted.  See Server.MaxConnectionRegisterAttempts
    */
  private[core] case class ConnectionRefused(channel: SocketChannel, attempt: Int)

  private[core] sealed trait ServerCommand
  private[core] case object Shutdown                          extends ServerCommand
  private[core] case class InitializerBroadcast(message: Any) extends ServerCommand
  private[core] case object GetInfo                           extends ServerCommand

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

    val serverStateAgent = new AtomicReference(ServerState(ConnectionVolumeState.Normal, ServerStatus.Initializing))

    val actor = io.actorSystem.actorOf(Props(classOf[Server], io, serverConfig, serverStateAgent)
                                         .withDispatcher("server-dispatcher"),
                                       name = s"server-${serverConfig.name.idString}")

    ServerRef(serverConfig, actor, io, serverStateAgent)
  }

}
