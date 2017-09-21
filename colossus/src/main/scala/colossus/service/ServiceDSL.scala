package colossus.service

import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration._
import scala.concurrent.Future
import scala.language.higherKinds
import java.net.InetSocketAddress

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
  implicit val worker = ctx.worker
  val server          = ctx.server

  def fullHandler: T => ServerConnectionHandler
}

trait ServiceInitializer[T] extends HandlerGenerator[T] {

  def onConnect: ServerContext => T

  def receive: ServerDSL.Receive = Map()
  def onShutdown() {}
}

trait ServiceDSL[T, I <: ServiceInitializer[T]] {

  def basicInitializer: InitContext => HandlerGenerator[T]

  class BridgeInitializer(init: InitContext, val serviceInitializer: I) extends Initializer(init) {
    def onConnect        = ctx => serviceInitializer.fullHandler(serviceInitializer.onConnect(ctx))
    override def receive = serviceInitializer.receive
    override def onShutdown() { serviceInitializer.onShutdown() }
  }

  def start(name: String, settings: ServerSettings)(init: InitContext => I)(implicit io: IOSystem): ServerRef = {
    Server.start(name, settings) { i =>
      new BridgeInitializer(i, init(i))
    }

  }

  def start(name: String, port: Int)(init: InitContext => I)(implicit io: IOSystem): ServerRef = {
    Server.start(name, port) { i =>
      new BridgeInitializer(i, init(i))
    }
  }

  def basic(name: String, port: Int, handler: ServerContext => T)(implicit io: IOSystem) = {
    Server.start(name, port) { i =>
      new Initializer(i) {
        val rinit     = basicInitializer(i)
        def onConnect = ctx => rinit.fullHandler(handler(ctx))
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

  def apply(host: String, port: Int)(implicit env: E): T = {
    apply(host, port, 1.second)
  }

  def apply(host: String, port: Int, requestTimeout: Duration)(implicit env: E): T = {
    apply(new InetSocketAddress(host, port), requestTimeout)
  }

  def apply(address: InetSocketAddress, requestTimeout: Duration)(implicit env: E): T = {
    val config = ClientConfig(
      address = address,
      requestTimeout = requestTimeout,
      name = MetricAddress.Root / defaultName
    )
    apply(config)
  }
}

trait ServiceClientFactory[P <: Protocol] extends ClientFactory[P, Callback, ServiceClient[P], WorkerRef] {

  implicit def clientTagDecorator: TagDecorator[P]
  
  def connectionHandler(base: ServiceClient[P], codec: Codec[Encoding.Client[P]]): ClientConnectionHandler = {
    new UnbindHandler(new Controller[Encoding.Client[P]](base, codec), base)
  }

  def codecProvider: Codec.Client[P]

  def apply(config: ClientConfig)(implicit worker: WorkerRef): ServiceClient[P] = {
    //TODO : binding a client needs to be split up from creating the connection handler
    // we should make a method called "create" the abstract method, and have
    // this apply call it, then move this to a more generic parent type
    val base    = new ServiceClient[P](config, worker.generateContext())
    val handler = connectionHandler(base, codecProvider)
    worker.worker ! WorkerCommand.Bind(handler)
    base
  }

}

object ServiceClientFactory {

  def basic[P <: Protocol](name: String, provider: () => Codec.Client[P],
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

  def clientConfig: Option[ClientConfig]
  def client: Sender[P, M]
  implicit val async: Async[M]

  def send(input: P#Request): M[P#Response] = client.send(input)

  protected def executeAndMap[T](i: P#Request)(f: P#Response => M[T]) = async.flatMap(send(i))(f)

  def disconnect() {
    client.disconnect()
  }

}

class BasicLiftedClient[P <: Protocol, M[_]](val client: Sender[P, M], val clientConfig: Option[ClientConfig])(
    implicit val async: Async[M])
    extends LiftedClient[P, M] {}
