package colossus
package core

object ServerDSL {
  type Receive = PartialFunction[Any, Unit]
}
import ServerDSL._

sealed trait MaybeHandler
object MaybeHandler {
  case object Reject extends MaybeHandler
  case class Lazy(constructor: HandlerConstructor) extends MaybeHandler
  case class Handler(handler: ServerConnectionHandler) extends MaybeHandler
}

trait ConnectionInitializer {

  def accept(handler: HandlerConstructor): MaybeHandler = MaybeHandler.Lazy(handler)
  def accept(handler: ServerConnectionHandler): MaybeHandler = MaybeHandler.Handler(handler)

  def reject(): MaybeHandler = MaybeHandler.Reject

}

trait HandlerConstructor {

  private[colossus] def createHandler(worker: WorkerRef): ServerConnectionHandler

}

trait WorkerInitializer {
  implicit val worker: WorkerRef

  def onConnect(init: ConnectionInitializer => MaybeHandler)

  //delegator message handling
  def receive(receiver: Receive)

}

class DSLDelegator(server : ServerRef, _worker : WorkerRef) extends Delegator(server, _worker) with WorkerInitializer {


  protected var currentReceiver: Receive = {case _ => ()}
  protected var currentAcceptor: ConnectionInitializer => MaybeHandler = x => MaybeHandler.Reject
  
  def onConnect(init: ConnectionInitializer => MaybeHandler) {
    currentAcceptor = init
  }


  def receive(r: Receive) {
    currentReceiver = r
  }

  def acceptNewConnection: Option[ServerConnectionHandler] = {
    currentAcceptor(new ConnectionInitializer {} ) match {
      case MaybeHandler.Reject     => None
      case MaybeHandler.Lazy(h)    => Some(h.createHandler(worker))
      case MaybeHandler.Handler(h) => Some(h)
    }
  }

  override def handleMessage: Receive = currentReceiver

}

//this is mixed in by Server
trait ServerDSL {

  def start(name: String, settings: ServerSettings)(initializer: WorkerInitializer => Unit)(implicit io: IOSystem) : ServerRef = {
    val serverConfig = ServerConfig(
      name = name,
      settings = settings,
      delegatorFactory = (s,w) => {
        val d = new DSLDelegator(s,w)
        //notice - the worker will catch any exceptions thrown in the user's function
        initializer(d)
        d
      }
    )
    Server(serverConfig)

  }

  def start(name: String, port: Int)(initializer: WorkerInitializer => Unit)(implicit io: IOSystem): ServerRef = start(name, ServerSettings(port))(initializer)


  def basic(name: String, port: Int, handlerFactory: () => ServerConnectionHandler)(implicit io: IOSystem): ServerRef = {
    start(name, port){context =>
      context onConnect { connection =>
        connection accept handlerFactory()
      }
    }
  }

}

