package colossus
package core

object ServerDSL {
  type Receive = PartialFunction[Any, Unit]
}
import ServerDSL._

trait ConnectionInitializer {

  def accept(handler: ServerConnectionHandler): Option[ServerConnectionHandler] = Some(handler)

  def reject(): Option[ServerConnectionHandler] = None

}

trait WorkerInitializer {
  def worker: WorkerRef

  def onConnect(init: ConnectionInitializer => Option[ServerConnectionHandler])

  //delegator message handling
  def receive(receiver: Receive)

  /* not sure if these should be here or not

  def clientFor[D <: CodecDSL](config: ClientConfig)(implicit provider: ClientCodecProvider[D]): ServiceClient[D#Input, D#Output] = {
    new ServiceClient(provider.clientCodec(), config, worker)
  }

  def clientFor[D <: CodecDSL](host: String, port: Int, requestTimeout: Duration = 1.second)(implicit provider: ClientCodecProvider[D]): ServiceClient[D#Input, D#Output] = {
    clientFor[D](new InetSocketAddress(host, port), requestTimeout)
  }

  def clientFor[D <: CodecDSL]
  (address: InetSocketAddress, requestTimeout: Duration)
  (implicit provider: ClientCodecProvider[D]): ServiceClient[D#Input, D#Output] = {
    val config = ClientConfig(
      address = address,
      requestTimeout = requestTimeout,
      name = MetricAddress.Root / provider.name
    )
    clientFor[D](config)
  }
  */
    
}

class DSLDelegator(server : ServerRef, worker : WorkerRef) extends Delegator(server, worker) with WorkerInitializer {

  protected var currentReceiver: Receive = {case _ => ()}
  protected var currentAcceptor: ConnectionInitializer => Option[ServerConnectionHandler] = x => None
  
  def onConnect(init: ConnectionInitializer => Option[ServerConnectionHandler]) {
    currentAcceptor = init
  }


  def receive(r: Receive) {
    currentReceiver = r
  }

  def acceptNewConnection: Option[ServerConnectionHandler] = {
    currentAcceptor(new ConnectionInitializer {} )
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

