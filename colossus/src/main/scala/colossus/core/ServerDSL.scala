package colossus
package core

object ServerDSL {
  type Receive = PartialFunction[Any, Unit]
}
import ServerDSL._

trait ConnectionInitializer {

  def accept(handler: HandlerConstructor): Option[HandlerConstructor] = Some(handler)

  def reject(): Option[HandlerConstructor] = None

}

trait HandlerConstructor {

  private[colossus] def createHandler(worker: WorkerRef): ServerConnectionHandler

}

trait WorkerInitializer {
  implicit val worker: WorkerRef

  def onConnect(init: ConnectionInitializer => Option[HandlerConstructor])

  //delegator message handling
  def receive(receiver: Receive)

}

class DSLDelegator(server : ServerRef, _worker : WorkerRef) extends Delegator(server, _worker) with WorkerInitializer {


  protected var currentReceiver: Receive = {case _ => ()}
  protected var currentAcceptor: ConnectionInitializer => Option[HandlerConstructor] = x => None
  
  def onConnect(init: ConnectionInitializer => Option[HandlerConstructor]) {
    currentAcceptor = init
  }


  def receive(r: Receive) {
    currentReceiver = r
  }

  def acceptNewConnection: Option[ServerConnectionHandler] = {
    currentAcceptor(new ConnectionInitializer {} ).map{_.createHandler(worker)}
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

}

