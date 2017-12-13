package colossus.service

import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration._
import scala.concurrent.Future
import scala.language.higherKinds
import java.net.InetSocketAddress

import akka.actor.Actor.Receive
import colossus.IOSystem
import colossus.controller.{Codec, Controller, Encoding}
import colossus.core.server.{Initializer, Server, ServerDSL}
import colossus.core._
import colossus.metrics.MetricAddress

trait Protocol {
  type Request
  type Response
}

abstract class HandlerGenerator[T](ctx: InitContext) {

  implicit val worker: WorkerRef = ctx.worker

  val server: ServerRef = ctx.server

  def fullHandler: T => ServerConnectionHandler
}

trait ServiceInitializer[T] extends HandlerGenerator[T] {

  type RequestHandlerFactory = ServerContext => T

  def onConnect: RequestHandlerFactory

  def receive: Receive = Map()

  def onShutdown() {}
}

trait ServiceDSL[T, I <: ServiceInitializer[T]] {

  def basicInitializer: InitContext => HandlerGenerator[T]

  class BridgeInitializer(init: InitContext, val serviceInitializer: I) extends Initializer(init) {
    override def onConnect: (ServerContext) => ServerConnectionHandler = { ctx =>
      serviceInitializer.fullHandler(serviceInitializer.onConnect(ctx))
    }

    override def receive: Receive = serviceInitializer.receive

    override def onShutdown(): Unit = {
      serviceInitializer.onShutdown()
    }
  }

  /**
    * Start service with an explicit [[colossus.core.ServerSettings]].
    *
    * @param name Service name
    * @param settings Settings that configure the service
    * @param init Initialization context to start the server
    * @param io I/O system where work is executed
    * @return Reference to the server
    */
  def start(name: String, settings: ServerSettings)(init: InitContext => I)(implicit io: IOSystem): ServerRef = {
    Server.start(name, settings) { i =>
      new BridgeInitializer(i, init(i))
    }
  }

  /**
    * Start service with a particular port, but other settings will be loaded from the configuration file.
    *
    * @param name Service name
    * @param port Port to open server on
    * @param init Initialization context to start the server
    * @param io I/O system where work is executed
    * @return Reference to the server
    */
  def start(name: String, port: Int)(init: InitContext => I)(implicit io: IOSystem): ServerRef = {
    Server.start(name, port) { i =>
      new BridgeInitializer(i, init(i))
    }
  }

  /**
    * Start service with settings loaded from the configuration file.
    *
    * @param name Service name
    * @param init Initialization context to start the server
    * @param io I/O system where work is executed
    * @return Reference to the server
    */
  def start(name: String)(init: InitContext => I)(implicit io: IOSystem): ServerRef = {
    Server.start(name) { i =>
      new BridgeInitializer(i, init(i))
    }
  }

  /**
    * Start a basic service with a particular port, but other settings will be loaded from the configuration file.
    *
    * @param name Service name
    * @param port Port to open server on
    * @param handler Logic that is applied to each request
    * @param io I/O system where work is executed
    * @return Reference to the server
    */
  def basic(name: String, port: Int, handler: ServerContext => T)(implicit io: IOSystem): ServerRef = {
    Server.start(name, port) { i =>
      new Initializer(i) {
        val requestInit = basicInitializer(i)

        def onConnect: ServerContext => ServerConnectionHandler = { ctx =>
          requestInit.fullHandler(handler(ctx))
        }
      }
    }
  }

}

/**
  * A one stop shop for a fully working Server DSL without any custom
  * functionality.  Just provide a codec and you're good to go
  */
trait BasicServiceDSL[P <: Protocol] {

  protected def provideCodec(): Codec.Server[P]

  protected def errorMessage(reason: ProcessingFailure[P#Request]): P#Response

  protected class ServiceHandler(rh: RequestHandler) extends ServiceServer[P](rh) {}

  protected class Generator(context: InitContext) extends HandlerGenerator[RequestHandler](context) {

    def fullHandler = rh => new PipelineHandler(new Controller(new ServiceHandler(rh), provideCodec()), rh)

  }

  abstract class Initializer(context: InitContext) extends Generator(context) with ServiceInitializer[RequestHandler]

  abstract class RequestHandler(ctx: ServerContext, config: ServiceConfig) extends GenRequestHandler[P](ctx, config) {
    def this(ctx: ServerContext) = this(ctx, ServiceConfig.load(ctx.name))

    def unhandledError = {
      case error => errorMessage(error)
    }
  }

  object Server extends ServiceDSL[RequestHandler, Initializer] {

    def basicInitializer = initContext => new Generator(initContext)

    def basic(name: String, port: Int)(handler: PartialFunction[P#Request, Callback[P#Response]])(
        implicit io: IOSystem) = start(name, port) { initContext =>
      new Initializer(initContext) {
        def onConnect = serverContext => new RequestHandler(serverContext) { def handle = handler }
      }
    }

  }

}

/**
  * This has to be implemented per codec in order to lift generic Sender traits to a type-specific trait
  *
  * For example this is how we go from ServiceClient[HttpRequest, HttpResponse] to HttpClient[Callback]
  */
trait ClientLifter[P <: Protocol, T[M[_]] <: Sender[P, M]] {

  def lift[M[_]](baseClient: Sender[P, M], clientConfig: Option[ClientConfig])(implicit async: Async[M]): T[M]

}

/**
  * A generic trait for creating clients.  There are several more specialized
  * subtypes that make more sense of the type parameters, so this trait should
  * generally not be used unless writing very generic code.
  *
  * Type Parameters:
  * * P - the protocol used by the client
  * * M[_] - the concurrency wrapper, either Callback or Future
  * * T - the type of the returned client
  * * E - an implicitly required environment type, WorkerRef for Callback and IOSystem for Future
  */
trait ClientFactory[P <: Protocol, M[_], +T <: Sender[P, M], E] {

  def defaultName: String

  protected lazy val configDefaults = ConfigFactory.load()

  /**
    * Load a ServiceClient definition from a Config.  Looks into `colossus.clients.$clientName` and falls back onto
    * `colossus.client-defaults`
    * @param clientName The name of the client definition to load
    * @param config A config object which contains at the least a `colossus.clients.$clientName` and a `colossus.client-defaults`
    * @return
    */
  def apply(clientName: String, config: Config = configDefaults)(implicit env: E): T = {
    apply(ClientConfig.load(clientName, config))
  }

  /**
    * Create a Client from a config source.
    *
    * @param config A Config object in the shape of `colossus.client-defaults`.  It is also expected to have the `address` and `name` fields.
    * @return
    */
  def apply(config: Config)(implicit env: E): T = {
    apply(ClientConfig.load(config))
  }

  def apply(config: ClientConfig)(implicit env: E): T

  def apply(hosts: Seq[InetSocketAddress])(implicit env: E): T = {
    // TODO default request timeout should come from config
    apply(hosts, 1.second)
  }

  def apply(host: String, port: Int)(implicit env: E): T = {
    // TODO default request timeout should come from config
    apply(host, port, 1.second)
  }

  def apply(host: String, port: Int, requestTimeout: Duration)(implicit env: E): T = {
    apply(Seq(new InetSocketAddress(host, port)), requestTimeout)
  }

  def apply(address: Seq[InetSocketAddress], requestTimeout: Duration)(implicit env: E): T = {
    val config = ClientConfig(
      address = address,
      requestTimeout = requestTimeout,
      name = MetricAddress.Root / defaultName
    )
    apply(config)
  }
}

trait ServiceClientFactory[P <: Protocol] extends ClientFactory[P, Callback, Client[P, Callback], WorkerRef] {

  implicit def clientTagDecorator: TagDecorator[P]

  def connectionHandler(base: ServiceClient[P], codec: Codec[Encoding.Client[P]]): ClientConnectionHandler = {
    new UnbindHandler(new Controller[Encoding.Client[P]](base, codec), base)
  }

  def codecProvider: Codec.Client[P]

  def apply(config: ClientConfig)(implicit worker: WorkerRef): Client[P, Callback] = {
    new LoadBalancer[P](config, this, new PrinciplePermutationGenerator[Client[P, Callback]](_))
  }

  def createClient(config: ClientConfig, address: InetSocketAddress)(
      implicit worker: WorkerRef): Client[P, Callback] = {
    //TODO : binding a client needs to be split up from creating the connection handler
    // we should make a method called "create" the abstract method, and have
    // this apply call it, then move this to a more generic parent type
    val base    = new ServiceClient[P](address, config, worker.generateContext())
    val handler = connectionHandler(base, codecProvider)
    worker.worker ! WorkerCommand.Bind(handler)
    base
  }
}

object ServiceClientFactory {

  def basic[P <: Protocol](name: String,
                           provider: () => Codec.Client[P],
                           tagDecorator: TagDecorator[P] = TagDecorator.default[P]) = new ServiceClientFactory[P] {

    def defaultName = name

    def codecProvider = provider()

    override def clientTagDecorator: TagDecorator[P] = tagDecorator
  }

}

class FutureClientFactory[P <: Protocol](base: FutureClient.BaseFactory[P])
    extends ClientFactory[P, Future, FutureClient[P], IOSystem] {

  def defaultName = base.defaultName

  def apply(config: ClientConfig)(implicit io: IOSystem) = FutureClient.create(config)(io, base)

}

class CodecClientFactory[P <: Protocol, M[_], T[M[_]] <: Sender[P, M], E](
    baseFactory: ClientFactory[P, M, Sender[P, M], E],
    lifter: ClientLifter[P, T],
    builder: AsyncBuilder[M, E])
    extends ClientFactory[P, M, T[M], E] {

  def defaultName: String = baseFactory.defaultName

  def apply(config: ClientConfig)(implicit env: E): T[M] = {
    apply(baseFactory(config), config)
  }

  def apply(sender: Sender[P, M], clientConfig: ClientConfig)(implicit env: E): T[M] = {
    lifter.lift(sender, Some(clientConfig))(builder.build(env))
  }

  def apply(sender: Sender[P, M])(implicit env: E): T[M] = {
    lifter.lift(sender, None)(builder.build(env))
  }
}

/**
  * Mixed into protocols to provide simple methods for creating clients.
  */
abstract class ClientFactories[P <: Protocol, T[M[_]] <: Sender[P, M]](lifter: ClientLifter[P, T]) {

  implicit def clientFactory: FutureClient.BaseFactory[P]

  implicit val futureFactory = new FutureClientFactory[P](clientFactory)

  val client = new CodecClientFactory[P, Callback, T, WorkerRef](
    clientFactory,
    lifter,
    AsyncBuilder.CallbackAsyncBuilder
  )

  val futureClient = new CodecClientFactory[P, Future, T, IOSystem](
    futureFactory,
    lifter,
    AsyncBuilder.FutureAsyncBuilder
  )

}

trait LiftedClient[P <: Protocol, M[_]] extends Sender[P, M] {

  def address: InetSocketAddress = client.address()
  def clientConfig: Option[ClientConfig]
  def client: Sender[P, M]
  implicit val async: Async[M]

  override def send(input: P#Request): M[P#Response] = client.send(input)

  protected def executeAndMap[T](i: P#Request)(f: P#Response => M[T]) = async.flatMap(send(i))(f)

  override def addInterceptor(interceptor: Interceptor[P]): Unit = client.addInterceptor(interceptor)

  override def disconnect(): Unit = client.disconnect()

  override def update(addresses: Seq[InetSocketAddress]): Unit = client.update(addresses)

}

class BasicLiftedClient[P <: Protocol, M[_]](val client: Sender[P, M], val clientConfig: Option[ClientConfig])(
    implicit val async: Async[M])
    extends LiftedClient[P, M] {}
