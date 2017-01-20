package colossus
package service

import com.typesafe.config.{Config, ConfigFactory}
import core._

import scala.concurrent.duration._
import scala.concurrent.Future
import scala.language.higherKinds

import java.net.InetSocketAddress
import metrics.MetricAddress
import controller.{Encoding, Codec, Controller}

trait Protocol {
  type Request
  type Response
}

abstract class HandlerGenerator[T](ctx: InitContext) {
  implicit val worker = ctx.worker
  val server = ctx.server


  def fullHandler: T => ServerConnectionHandler
}

trait ServiceInitializer[T] extends HandlerGenerator[T] {

  def onConnect: ServerContext => T
}

trait ServiceDSL[T, I <: ServiceInitializer[T]] {

  def basicInitializer: InitContext => HandlerGenerator[T]

  def start(name: String, settings: ServerSettings)(init: InitContext => I)(implicit io: IOSystem): ServerRef = {
    Server.start(name, settings){i => new core.Initializer(i) {
      val rinit = init(i)
      def onConnect = ctx => rinit.fullHandler(rinit.onConnect(ctx))
    }}

  }

  def start(name: String, port: Int)(init: InitContext => I)(implicit io: IOSystem): ServerRef = {
    Server.start(name, port){i => new core.Initializer(i) {
      val rinit = init(i)
      def onConnect = ctx => rinit.fullHandler(rinit.onConnect(ctx))
    }}
  }

  def basic(name: String, port: Int, handler: ServerContext => T)(implicit io: IOSystem) = {
    Server.start(name, port){i => new core.Initializer(i) {
      val rinit = basicInitializer(i)
      def onConnect = ctx => rinit.fullHandler(handler(ctx))
    }}
  }

}

/**
 * A one stop shop for a fully working Server DSL without any custom
 * functionality.  Just provide a codec and you're good to go
 */
trait BasicServiceDSL[P <: Protocol] {
  import controller.Controller

  protected def provideCodec(): Codec.Server[P]

  protected def errorMessage(reason: ProcessingFailure[P#Request]): P#Response

  protected class ServiceHandler(rh: RequestHandler) extends ServiceServer[P](rh) {

  }

  protected class Generator(context: InitContext) extends HandlerGenerator[RequestHandler](context) {

    def fullHandler = rh => new PipelineHandler(new Controller(new ServiceHandler(rh), provideCodec()), rh)

  }

  abstract class Initializer(context: InitContext) extends Generator(context) with ServiceInitializer[RequestHandler]

  abstract class RequestHandler(ctx: ServerContext, config: ServiceConfig ) extends GenRequestHandler[P](config, ctx){
    def this(ctx: ServerContext) = this(ctx, ServiceConfig.load(ctx.name))

    def unhandledError = {
      case error => errorMessage(error)
    }
  }

  object Server extends ServiceDSL[RequestHandler, Initializer] {

    def basicInitializer = new Generator(_)

    def basic(name: String, port: Int)(handler: PartialFunction[P#Request, Callback[P#Response]])(implicit io: IOSystem) = start(name, port){new Initializer(_) {
      def onConnect = new RequestHandler(_) { def handle = handler }
    }}

  }

}


  

/**
 * This has to be implemented per codec in order to lift generic Sender traits to a type-specific trait
 *
 * For example this is how we go from ServiceClient[HttpRequest, HttpResponse] to HttpClient[Callback]
 */
trait ClientLifter[C <: Protocol, T[M[_]] <: Sender[C,M]] {

  def lift[M[_]](baseClient: Sender[C, M], clientConfig: Option[ClientConfig])(implicit async: Async[M]) : T[M]

}

trait ClientFactory[C <: Protocol, M[_], +T <: Sender[C,M], E] {

  def defaultName: String


  protected lazy val configDefaults = ConfigFactory.load()

  /**
    * Load a ServiceClient definition from a Config.  Looks into `colossus.clients.$clientName` and falls back onto
    * `colossus.client-defaults`
    * @param clientName The name of the client definition to load
    * @param config A config object which contains at the least a `colossus.clients.$clientName` and a `colossus.client-defaults`
    * @return
    */
  def apply(clientName : String, config : Config = configDefaults)(implicit env: E) : T = {
    apply(ClientConfig.load(clientName, config))
  }

  /**
    * Create a Client from a config source.
    *
    * @param config A Config object in the shape of `colossus.client-defaults`.  It is also expected to have the `address` and `name` fields.
    * @return
    */
  def apply(config : Config)(implicit  env: E) : T = {
    apply(ClientConfig.load(config))
  }

  def apply(config: ClientConfig)(implicit env: E): T

  def apply(host: String, port : Int)(implicit env: E): T = {
    apply(host, port, 1.second)
  }

  def apply(host: String, port: Int, requestTimeout: Duration)(implicit env: E): T = {
    apply(new InetSocketAddress(host, port), requestTimeout)
  }

  def apply (address: InetSocketAddress, requestTimeout: Duration) (implicit env: E): T = {
    val config = ClientConfig(
      address = address,
      requestTimeout = requestTimeout,
      name = MetricAddress.Root / defaultName
    )
    apply(config)
  }
}

trait ServiceClientFactory[P <: Protocol] extends ClientFactory[P, Callback, ServiceClient[P], WorkerRef] {

  def connectionHandler(base: ServiceClient[P], codec: Codec[Encoding.Client[P]]) : ClientConnectionHandler = {
    new UnbindHandler(new Controller[Encoding.Client[P]](base, codec), base)
  }

  def codecProvider: Codec.Client[P]

  def apply(config: ClientConfig)(implicit worker: WorkerRef): ServiceClient[P] = {
    //TODO : binding a client needs to be split up from creating the connection handler
    // we should make a method called "create" the abstract method, and have
    // this apply call it, then move this to a more generic parent type
    val base = new ServiceClient[P](config, worker.generateContext())
    val handler = connectionHandler(base, codecProvider)
    worker.worker ! WorkerCommand.Bind(handler)
    base
  }

}


object ServiceClientFactory {
  
  def basic[P <: Protocol](name: String, provider: () => Codec.Client[P]) = new ServiceClientFactory[P] {

    def defaultName = name

    def codecProvider = provider()

  }

}

class FutureClientFactory[P <: Protocol](base: FutureClient.BaseFactory[P]) extends ClientFactory[P, Future, FutureClient[P], IOSystem] {

  def defaultName = base.defaultName
  
  def apply(config: ClientConfig)(implicit io: IOSystem) = FutureClient.create(config)(io, base)

}

class CodecClientFactory[C <: Protocol, M[_], T[M[_]] <: Sender[C,M], E]
(implicit baseFactory: ClientFactory[C, M,Sender[C,M],E], lifter: ClientLifter[C,T], builder: AsyncBuilder[M,E])
extends ClientFactory[C,M,T[M],E] {

  def defaultName = baseFactory.defaultName

  def apply(config: ClientConfig)(implicit env: E): T[M] = {
    apply(baseFactory(config), config)
  }

  def apply(sender: Sender[C,M], clientConfig: ClientConfig)(implicit env: E): T[M] = {
    lifter.lift(sender, Some(clientConfig))(builder.build(env))
  }

  def apply(sender: Sender[C,M])(implicit env: E): T[M] = {
    lifter.lift(sender, None)(builder.build(env))
  }
}

/**
 * Mixed into protocols to provide simple methods for creating clients.
 */
abstract class ClientFactories[C <: Protocol, T[M[_]] <: Sender[C, M]](implicit lifter: ClientLifter[C,T]) {

  implicit def clientFactory: FutureClient.BaseFactory[C]

  implicit val futureFactory = new FutureClientFactory[C](clientFactory)

  val client = new CodecClientFactory[C, Callback, T, WorkerRef]

  val futureClient = new CodecClientFactory[C, Future, T, IOSystem]

}

trait LiftedClient[C <: Protocol, M[_] ] extends Sender[C,M] {

  def clientConfig: Option[ClientConfig]
  def client: Sender[C,M]
  implicit val async: Async[M]

  def send(input: C#Request): M[C#Response] = client.send(input)

  protected def executeAndMap[T](i : C#Request)(f : C#Response => M[T]) = async.flatMap(send(i))(f)

  def disconnect() {
    client.disconnect()
  }

}

class BasicLiftedClient[C <: Protocol, M[_] ](val client: Sender[C,M], val clientConfig: Option[ClientConfig])
                                             (implicit val async: Async[M]) extends LiftedClient[C,M] {

}
