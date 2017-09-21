package colossus.core

import java.util.concurrent.atomic.AtomicReference

import akka.actor.{ActorRef, PoisonPill}
import akka.pattern.ask
import akka.util.Timeout
import colossus.IOSystem

import scala.concurrent.duration._
import scala.concurrent.Future
import colossus.metrics._
import server._

/**
  * A `ServerRef` is a handle to a created server.  It can be used to get basic
  * information about the state of the server as well as send operational
  * commands to it.
  *
  * @param config The ServerConfig used to create this Server
  * @param server The ActorRef of the Server
  * @param system The IOSystem to which this Server belongs
  * @param serverStateRef The current state of the Server.
  */
case class ServerRef private[colossus] (config: ServerConfig,
                                        server: ActorRef,
                                        system: IOSystem,
                                        private val serverStateRef: AtomicReference[ServerState]) {
  def name = config.name

  def serverState = serverStateRef.get()

  val namespace: MetricNamespace = system.namespace / name

  def maxIdleTime = {
    if (serverStateRef.get().connectionVolumeState == ConnectionVolumeState.HighWater) {
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
    * Broadcast a message to a all of the [[Initializer]]s of this server.
    *
    * @param message Message to broadcast to the server's [[Initializer]]s
    * @param sender Reference to the Actor who sent the message
    * @return
    */
  @deprecated("function has been deprecated, please use `initializerBroadcast` instead", "0.9.0")
  def delegatorBroadcast(message: Any)(implicit sender: ActorRef = ActorRef.noSender) {
    initializerBroadcast(message)
  }

  def initializerBroadcast(message: Any)(implicit sender: ActorRef = ActorRef.noSender) {
    server.!(Server.InitializerBroadcast(message))(sender)
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
