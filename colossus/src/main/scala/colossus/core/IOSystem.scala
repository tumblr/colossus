package colossus

import akka.util.Timeout
import colossus.core.Worker.ConnectionSummary
import com.typesafe.config.{ConfigFactory, Config}

import core._

import akka.actor._
import metrics._
import task._

import java.net.InetSocketAddress

import scala.concurrent.{ExecutionContext, Future}

object IOSystem {

  val ConfigRoot = "colossus.io-system"

  /**
    * Create a new IOSystem, using only the defaults provided by the corresponding "colossus.io-system" config path.
    * A Config object will be created via {{{ConfigFactory.load()}}}
    * @param sys
    * @return
    */
  def apply()(implicit sys : ActorSystem) : IOSystem = {
    apply(ConfigRoot)
  }

  /**
    * Create a new IOSystem using the supplied configPath.  This configPath will be overlaid on top of the default "colossus.io-system"
    * config path.
    * A Config object will be created via {{{ConfigFactory.load()}}}
    *
    * @param configPath  The path to the configuration.
    * @param sys
    * @return
    */
  def apply(configPath : String)(implicit sys : ActorSystem) : IOSystem = {
    apply(configPath, ConfigFactory.load())
  }

  /**
    * Create a new IOSystem by loading its config from the specified configPath.  This configPath will be overlaid on top of the default "colossus.io-system" config path.
    * @param configPath The path to the configuration
    * @param config The Config source to query
    * @param sys
    * @return
    */
  def apply(configPath : String, config : Config)(implicit sys : ActorSystem) : IOSystem = {
    val ioConfig = config.getConfig(configPath).withFallback(config.getConfig(ConfigRoot))
    val name = ioConfig.getString("name")
    val workerCount : Option[Int] = if(config.hasPath("num-workers")){Some(config.getInt("num-workers"))} else{ None }
    val ms = MetricSystem(s"$configPath.metrics", config)
    apply(name, workerCount, ms)
  }

  /**
    * Create a new IOSystem
    * @param name Name of this IOSystem.  This will also be used as its MetricAddres.
    * @param workerCount Number of workers to create
    * @param metrics The MetricSystem used to report metrics
    * @param system
    * @return
    */
  def apply(name : String, workerCount : Option[Int], metrics : MetricSystem)(implicit system : ActorSystem) : IOSystem = {
    val numWorkers = workerCount.getOrElse(Runtime.getRuntime.availableProcessors())

    val workerManager = system.actorOf(Props(classOf[WorkerManager], WorkerManagerConfig(numWorkers, metrics)), s"iosystem-${actorFriendlyName(name)}-manager")
    val sys = IOSystem(workerManager, name, numWorkers, metrics, system)
    workerManager ! WorkerManager.Initialize(sys)
    sys
  }
  private def actorFriendlyName(name : String) = {
    name match {
      case "" | "/" => ""
      case s => s.replace("/", "-")
    }
  }
}

/**
 * An IO System is basically a collection of worker actors.  You can attach
 * either servers or clients to an IOSystem.
 *
 * You may have multiple IOSystems per ActorSystem and actors attached to
 * different IOSystems have no rules about cross communication.  The main thing
 * to keep in mind is that all actors in a single IOSystem will share event
 * loops.
 */
case class
IOSystem private[colossus](workerManager: ActorRef, name : String, numWorkers : Int, metrics: MetricSystem, actorSystem: ActorSystem) {
  import IOCommand._

  import akka.pattern.ask
  import colossus.core.WorkerManager.RegisteredServers

  /**
   * MetricAddress of this IOSystem as seen from the MetricSystem
   */
  val namespace = metrics.namespace / name

  /**
   * Sends a message to the underlying WorkerManager.
   * @param msg Msg to send
   * @param sender The sender of the message, defaults to ActorRef.noSender
   * @return
   */
  def ! (msg: Any)(implicit sender: ActorRef = ActorRef.noSender) = workerManager ! msg

  /**
   * Shuts down the IO System
   */
  def shutdown() {
    workerManager ! WorkerManager.Shutdown
  }
  /**
   * Shuts down the entire actor system
   */
  def apocalypse() {
    workerManager ! WorkerManager.Apocalypse
  }

  def registeredServers(implicit to : Timeout, ec : ExecutionContext) : Future[Seq[ServerRef]] = {
    (workerManager ? WorkerManager.ListRegisteredServers).mapTo[RegisteredServers].map(_.servers)
  }

  def connectionSummary(implicit to : Timeout, ec :ExecutionContext) : Future[ConnectionSummary] = {
    (workerManager ? WorkerManager.GetConnectionSummary).mapTo[ConnectionSummary]
  }

  /**
   * Run the specified task on a Worker thread.
   * @param t The Task to run
   * @return The Task's ActorRef
   */
  def run(t: Task): ActorRef = {
    workerManager ! IOCommand.BindWorkerItem(_ => t)
    t.proxy
  }

  /**
   * Connect to the specified address and use the specified function to create a Handler to attach to it.
   * @param address The address with which to connect.
   * @param handler Function which takes in the bound WorkerRef and creates a ClientConnectionHandler which is connected
   *                to the handler.
   */
  def connect(address: InetSocketAddress, handler: Context => ClientConnectionHandler) {
    workerManager ! BindAndConnectWorkerItem(address, handler)
  }
}

/**
 * An IO Command is sent to an IO System, which then routes the command to a
 * random worker using a round-robin router.  If you want more determinism over
 * how items are distributed across workers, just use multiple IO Systems
 *
 * TODO: It may be a better idea to move these into WorkerCommand with a tag
 * trait to show they can be accepted by the IOSystem, and then also add
 * methods onto the IOSystem to eliminate confusion
 */
sealed trait IOCommand

object IOCommand {

  /**
   * Bind a WorkerItem to a worker and then begin establishing an outgoing
   * connection whose events will be sent to the item.
   *
   * Notice that this is different from Worker.Connect, which must be sent to a
   * specific worker and can only be used on a WorkerItem already bound to that
   * worker
   */
  case class BindAndConnectWorkerItem(address: InetSocketAddress, item: Context => WorkerItem) extends IOCommand

  /**
   * Bind a [WorkerItem] to a Worker.  Multiple Bind messages are round
   * robined across workers in an IOSystem
   */
  case class BindWorkerItem(item: Context => WorkerItem) extends IOCommand

}
