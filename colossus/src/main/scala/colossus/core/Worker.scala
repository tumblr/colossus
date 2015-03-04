package colossus
package core

import akka.actor._
import akka.event.LoggingAdapter
import metrics._
import service.CallbackExecution

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, SocketChannel}

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.util.control.NonFatal



/**
 * Contains the configuration for each Worker. Created when Workers are spawned by the WorkerManager.
 * Notice - currently the worker config cannot contain the MetricSystem,
 * because workers are created as a part of creating the MetricSystem
 *
 * @param io The IOSystem to which this Worker belongs
 * @param workerId This Worker's unique id amongst its peers.
 */
case class WorkerConfig(
  io: IOSystem,
  workerId: Int
)

/**
 * This is a Worker's public interface.  This is what can be used to communicate with a Worker, as it
 * wraps the Worker's ActorRef, as well as providing some additional information which can be made public.
 * @param id The Worker's id.
 * @param metrics The Metrics associated with this Worker
 * @param worker The ActorRef of the Worker
 * @param system The IOSystem to which this Worker belongs
 */
case class WorkerRef(id: Int, metrics: LocalCollection, worker: ActorRef, system: IOSystem) {
  /**
   * Send this Worker a message
   * @param message The message to send
   * @param sender  The sendef of the message
   * @return
   */
  def !(message: Any)(implicit sender: ActorRef = ActorRef.noSender)  = worker ! message

  /**
   * Bind a new worker item to this worker.  The item should have been created
   * and initialized within this worker to ensure that the worker item's
   * lifecycle is single-threaded.
   */
  def bind(item: WorkerItem) {
    worker ! IOCommand.BindWorkerItem(_ => item)
  }

  def unbind(workerItemId: Long) {
    worker ! WorkerCommand.UnbindWorkerItem(workerItemId)
  }

  /**
   * The representation of this worker as a [CallbackExecutor].  Bring this
   * into scope to easily convert Futures into Callbacks.  This uses the
   * default dispatcher of the worker's underlying ActorSystem.
   */
  implicit val callbackExecutor = service.CallbackExecutor(system.actorSystem.dispatcher, worker)
}

/**
 * This keeps track of all the bound worker items, and properly handles added/removing them
 */
class WorkerItemManager(worker: WorkerRef, log: LoggingAdapter) {

  private val workerItems = collection.mutable.Map[Long, WorkerItem]()

  private var id: Long = 0L
  def newId(): Long = {
    id += 1
    id
  }

  def get(id: Long): Option[WorkerItem] = workerItems.get(id)

  /**
   * Binds a new worker item to this worker
   */
  def bind(workerItem: WorkerItem) {
    if (workerItem.isBound) {
      log.error(s"Attempted to bind worker ${workerItem.binding} that was already bound")
    } else {
      val id = newId()
      workerItems(id) = workerItem
      workerItem.setBind(id, worker)
    }
  }

  def unbind(id: Long) {
    if (workerItems contains id) {
      val item = workerItems(id)
      workerItems -= id
      item.setUnbind()
    } else {
      log.error(s"Attempted to unbind worker $id that is not bound to this worker")
    }
  }

  def unbind(workerItem: WorkerItem) {
    workerItem.id.map{i =>
      unbind(i)
    }.getOrElse(
      //maybe throw an exception instead?
      log.error("Attempted to unbind worker item that was already not bound!")
    )
  }
}



private[colossus] class Worker(config: WorkerConfig) extends Actor with ActorMetrics with ActorLogging with CallbackExecution {
  import Server._
  import Worker._
  import WorkerManager._
  import config._

  val parent = context.parent
  var trace = true

  val watchedConnections = collection.mutable.Map[ActorRef, ClientConnection]()

  //these ids are used for both connections and worker items so a connection
  //and it's attached handler will have different ids
  //TODO - we can probably improve performance by making ids a case class containing the item as a private field
  private var id: Long = 0L
  def newId(): Long = {
    id += 1
    id
  }

  implicit val mylog = log
  val metricSystem = config.io.metrics
  override def globalTags = Map("worker" -> (io.name + "-" + workerId.toString))

  val eventLoops              = metrics getOrAdd Rate(io.namespace / "worker" / "event_loops", List(1.second))
  val numConnections          = metrics getOrAdd Counter(io.namespace / "worker" / "connections")
  val rejectedConnections     = metrics getOrAdd Rate(io.namespace / "worker" / "rejected_connections", List(1.second, 60.seconds))

  val selector: Selector = Selector.open()
  val buffer = ByteBuffer.allocateDirect(1024 * 128)


  //collection of all the active connections
  val connections = collection.mutable.Map[Long, Connection]()


  //mapping of registered servers to their delegators
  val delegators = collection.mutable.Map[ActorRef, Delegator]()

  val me = WorkerRef(workerId, metrics, self, io)

  //collection of all the bound WorkerItems, including connection handlers
  val workerItems = new WorkerItemManager(me, log)
  def getWorkerItem(id: Long): Option[WorkerItem] = workerItems.get(id)

  override def preStart() {
    super.preStart()
    log.debug(s"starting worker ${config.workerId}")
    parent ! WorkerReady
    self ! Select
  }

  def receive = handleCallback orElse accepting

  def accepting: Receive = handleMetrics orElse {
    case Select => {
      selectLoop()
      self ! Select
    }
    case c: IOCommand => handleIOCommand(c)
    case c: WorkerCommand => handleWorkerCommand(c)
    case CheckIdleConnections => {
      val time = System.currentTimeMillis
      val timedOut = connections.filter{case (_, con) =>
        con.handler.idleCheck(WorkerManager.IdleCheckFrequency)
        con.isTimedOut(time)
      }.toList
      timedOut.foreach{case (_, con) =>
        unregisterConnection(con, DisconnectCause.TimedOut)
      }
      if (timedOut.size > 0) {
        log.debug(s"Terminated ${timedOut.size} idle connections")
        //TODO: Tick Metric
      }
    }
    case WorkerManager.RegisterServer(server, factory, timesTried) => if (!delegators.contains(server.server)){
      try{
        delegators(server.server) = factory(server, me)
        log.debug(s"registered server ${server.name}")
        sender ! ServerRegistered
      }catch {
        case NonFatal(e) => {
          log.error(e, s"failed to create delegator")
          sender ! RegistrationFailed
        }
      }
    } else {
      log.warning("attempted to re-register a server")
      sender ! WorkerManager.ServerRegistered
    }
    case WorkerManager.UnregisterServer(server) => {
      unregisterServer(server.server)
    }
    case DelegatorMessage(server, message) => {
      delegators
        .find{case (_, delegator) => delegator.server == server}
        .map{case (_, delegator) =>
          delegator.handleMessage.orElse[Any, Unit] {
            case unhandled => log.warning(s"Unhandled message $unhandled for delegator of server ${server.name}")
          }(message)
        }
        .getOrElse{
          log.error(s"delegator message $message for unknown server ${server.name}")
        }
    }
    case NewConnection(sc, attempt) => delegators.get(sender()).map{delegator =>
      delegator.createConnectionHandler.map{handler =>
        registerConnection(sc, delegator.server, handler)
      }.getOrElse{
        sc.close()
        delegator.server.server ! Server.ConnectionClosed(0, DisconnectCause.Unhandled)
        rejectedConnections.hit(tags = Map("server" -> delegator.server.name.idString))
      }
    }.getOrElse{
      //TODO: do something with the connection
      sender ! Server.ConnectionRefused(sc, attempt)
      log.error("Received connection from unregistered server!!!")
    }
    case Terminated(handler) => {
      if (watchedConnections contains handler) {
        watchedConnections(handler).close(DisconnectCause.Disconnect) //TODO: Is this right?  Should I be sending Terminated?
        watchedConnections -= handler
      }
    }
    case ConnectionSummaryRequest => {
      val now = System.currentTimeMillis //save a few thousand calls by doing this
      sender ! ConnectionSummary(connections.values.map{_.info(now)}.toSeq)
    }
  }

  def handleIOCommand(cmd: IOCommand) {
    import IOCommand._
    cmd match {
      case BindWorkerItem(itemFactory) => {
        workerItems.bind(itemFactory(me))
      }
      case BindAndConnectWorkerItem(address, itemFactory) => {
        val item = itemFactory(me)
        workerItems.bind(item)
        self ! WorkerCommand.Connect(address, item.id.get)
      }
    }
  }



  //start the connection process for either a new client or a reconnecting client
  def clientConnect(address: InetSocketAddress, handler: ClientConnectionHandler) {
    val channel = SocketChannel.open()
    channel.configureBlocking(false)
    channel.connect(address)
    val key = channel.register(selector, SelectionKey.OP_CONNECT)
    val connection = new ClientConnection(newId(), key, channel, handler)
    key.attach(connection)
    connections(connection.id) = connection
    numConnections.increment()
    handler match {
      case w: WatchedHandler => {
        watchedConnections(w.watchedActor) = connection
        context.watch(w.watchedActor)
      }
      case _ =>{}
    }
  }


  def handleWorkerCommand(cmd: WorkerCommand){
    import WorkerCommand._
    cmd match {
      case Message(wid, message) => {
        val responder = sender()
        getWorkerItem(wid).map{item =>
          item.receivedMessage(message, responder)
        }.getOrElse{
          log.error(s"worker received message $message for unknown item $wid")
          responder ! MessageDeliveryFailed(wid, message)
        }
      }
      case s: Schedule => {
        //akka's scheduler doesn't work when an actor is looping messages to
        //itself in a pinned dispatcher (trust me I've tried), so we'll send this
        //to the manager to do it for us this is mostly for scheduling reconnects
        //(yes we have the TaskScheduler, but lets try to get that out of here)
        context.parent ! s
      }
      case UnbindWorkerItem(id) => {
        workerItems.unbind(id)
      }
      case Disconnect(connectionId) => {
        connections.get(connectionId).foreach{con =>
          unregisterConnection(con, DisconnectCause.Disconnect)
        }
      }
      case Connect(address, id) => {
        getWorkerItem(id).map{
          case handler: ClientConnectionHandler => try {
            clientConnect(address, handler)
          } catch {
            case t: Throwable => {
              log.error(s"Failed to establish connection to $address: $t")
              handler.connectionFailed()
            }
          }
          case other => log.error(s"Attempted to attach connection ($address) to a worker item that's not a ClientConnectionHandler")
        }.getOrElse {
          log.error(s"Attempted to attach connection (${address}) to non-existant WorkerItem $id")
        }
      }
    }
  }

  /**
   * Registers a new server connection
   */
  def registerConnection(sc: SocketChannel, server: ServerRef, handler: ConnectionHandler) {
      val newKey: SelectionKey = sc.register( selector, SelectionKey.OP_READ )
      val connection = new ServerConnection(newId(), newKey, sc, handler, server)(self)
      newKey.attach(connection)
      connections(connection.id) = connection
      numConnections.increment()
      workerItems.bind(handler)
      handler.connected(connection)
  }

  /** Removes a closed connection and possibly unbinds the associated Connection Handler
    * 
    * In general, the only time we don't want to unbind the handler is when the
    * connection is a client connection with the ManualUnbindHandler mixed in
    * and the disconnect cause is an error.  This allows the client to possibly
    * reconnect and still receive messages.
    *
    * Here's the table 
    *
    *   Server/Client   ManualUnbindHandler   DisconnectError   Unbind?
    *         S                 -                   -             Y
    *         C                 N                   -             Y
    *         C                 Y                   N             Y
    *         C                 Y                   Y             N
    *
    * It may be the case in the future that we want the ManualUnbindHandler to
    * cause the handler to stay bound even on non-error disconnects, but for
    * now the only reason that mixin exists is for ServiceClient reconnections,
    * so it makes sense for now to err on the side of caution and not risk a
    * leak of bound WorkerItems
    */
  def unregisterConnection(con: Connection, cause : DisconnectCause) {
    connections -= con.id
    con.close(cause)
    numConnections.decrement()
    (con.handler, cause) match {
      case (m: ManualUnbindHandler, d: DisconnectError) => {}
      case _ => workerItems.unbind(con.handler)
    }
  }

  def unregisterServer(handler: ActorRef) {
    if (delegators contains handler) {
      val delegator = delegators(handler)
      val closed = connections.collect{
        case (_, s: ServerConnection) if (s.server == delegator.server) => {
          unregisterConnection(s, DisconnectCause.Terminated)
        }
      }
      delegators -= handler
      delegator.onShutdown()
      log.info(s"unregistering server ${delegator.server.name} (terminating ${closed.size} associated connections)")
    } else {
      log.warning(s"Attempted to unregister unknown server actor ${handler.path.toString}")
    }
  }

  def selectLoop() {
    val num = selector.select(1) //need short wait times to register new connections
    eventLoops.hit()
    implicit val TIME = System.currentTimeMillis
    val selectedKeys = selector.selectedKeys()
    val it = selectedKeys.iterator()
    while (it.hasNext) {
      val key : SelectionKey = it.next
      if (!key.isValid) {
        log.error("KEY IS INVALID")
        it.remove()
      } else if (key.isConnectable) {
        val con = key.attachment.asInstanceOf[ClientConnection]
        try {
          con.handleConnected()
        } catch {
          case t: Throwable => {
            con.clientHandler.connectionFailed()
            if (!con.clientHandler.isInstanceOf[ManualUnbindHandler]) {
              workerItems.unbind(con.clientHandler)
            }
            //only ConnectException is expected here, other exceptions should be logged
            t match {
              case c: java.net.ConnectException => {}
              case _ => log.error(s"Unexpected exception occurred while completing connection: ${t.getMessage}")
            }
          }
        }
        it.remove()
      }  else if (key.isReadable) {
        // Read the data
        buffer.clear
        val sc: SocketChannel = key.channel().asInstanceOf[SocketChannel]
        try {
          val len = sc.read(buffer)
          if (len > -1) {
            key.attachment match {
              case connection: Connection => {
                buffer.flip
                val data = DataBuffer(buffer, len)
                connection.handleRead(data)
              } //end case
            }
          } else {
            //reading -1 bytes means the connection has been closed
            key.attachment match {
              case c: Connection => {
                unregisterConnection(c, DisconnectCause.Closed)
                key.cancel()
              }
            }
          }
        } catch {
          case t: java.io.IOException => {
            key.attachment match {
              case c: Connection => {
                //connection reset by peer, sometimes thrown by read when the connection closes
                unregisterConnection(c, DisconnectCause.Closed)
              }
            }
            sc.close()
            key.cancel()
          }
          case t: Throwable => {
            log.warning(s"Unknown Error! : ${t.getClass.getName}: ${t.getMessage}")
            if (trace) {
              t.printStackTrace()
            }
            //close the connection to ensure it's not in an undefined state
            key.attachment match {
              case c: Connection => {
                log.warning(s"closing connection ${c.id} due to unknown error")
                unregisterConnection(c, DisconnectCause.Error(t))
              }
              case other => {
                log.error (s"Key has bad attachment!! $other")
              }
            }
            sc.close()
            key.cancel()
          }
        }
        it.remove()
      } else if (key.isWritable) {
        key.attachment  match {
          case c: Connection => try {
            c.handleWrite()
          } catch {
            case j: java.io.IOException => {
              unregisterConnection(c, DisconnectCause.Error(j))
            }
            case other: Throwable => {
              log.warning(s"Error handling write: ${other.getClass.getName} : ${other.getMessage}")
            }
          }
          case _ => {}
        }
      }
    }
  }

  override def postStop() {
    //cleanup
    selector.keys.foreach{_.channel.close}
    connections.foreach{case (_, con) =>
      con.close(DisconnectCause.Terminated)
    }
    delegators.foreach{ case (server, delegator) =>
      delegator.onShutdown()
    }
    selector.close()
    log.info("PEACE OUT")
  }

}

/**
 * Like the server actor, it is critical that instances of this actor get their
 * own thread, since they block when waiting for events.
 *
 * Workers are the "engine" of Colossus.  They are the components which receive and operate on Connections,
 * respond to java NIO select loop events and execute the corresponding reactors in the Delegator and ConnectionHandlers.
 *
 * These are the messages to which a Worker will respond.
 */
//NOTE: we should really divide this and WorkerCommand into public and private classes.
object Worker {
  private[core] case object ConnectionSummaryRequest
  private[core] case object CheckIdleConnections

  case class ConnectionSummary(infos: Seq[ConnectionInfo])

  /** Sent from Servers
   * @param sc the underlying socketchannel of the connection
   * @param attempt used when a worker refuses a connection, which can happen if a worker has just restarted and hasn't yet re-registered servers
   */
  private[core] case class NewConnection(sc: SocketChannel, attempt: Int = 1)

  /**
   * Send a message to the delegator belonging to the server
   */
  case class DelegatorMessage(server: ServerRef, message: Any)

  case class MessageDeliveryFailed(id: Long, message: Any)
}

/**
 * These are a different class of Commands to which a worker will respond.  These
 * are more relevant to the lifecycle of WorkerItems
 */
sealed trait WorkerCommand
object WorkerCommand {

  //open a new client connection to the specified address.  The bound
  //[WorkerItem] with id `id` will act as the event handler for the connection
  case class Connect(address: InetSocketAddress, id: Long) extends WorkerCommand
  case class UnbindWorkerItem(id: Long) extends WorkerCommand
  case class Schedule(in: FiniteDuration, message: Any) extends WorkerCommand
  case class Message(id: Long, message: Any) extends WorkerCommand
  case class Disconnect(id: Long) extends WorkerCommand
}

