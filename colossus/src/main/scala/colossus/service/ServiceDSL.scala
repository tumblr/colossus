package colossus
package service

import com.typesafe.config.{Config, ConfigFactory}
import core._

import akka.actor.ActorRef
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.language.higherKinds

import java.net.InetSocketAddress
import metrics.MetricAddress
import controller.{Encoding, Codec}

trait Protocol {
  type Request
  type Response
}

object Protocol {

  type PartialHandler[C <: Protocol] = PartialFunction[C#Request, Callback[C#Response]]

  type Receive = PartialFunction[Any, Unit]

  type ErrorHandler[C <: Protocol] = PartialFunction[ProcessingFailure[C#Request], C#Response]

  type ParseErrorHandler[C <: Protocol] = PartialFunction[Throwable, C#Response]
}

import Protocol._

class UnhandledRequestException(message: String) extends Exception(message)
class ReceiveException(message: String) extends Exception(message)

abstract class DSLService[C <: Protocol](val requestHandler: GenRequestHandler[C]) 
extends ServiceServer[C](requestHandler.config) 
with DownstreamEventHandler[GenRequestHandler[C]] { 

  downstream.setUpstream(this)

  override def onBind() {
    requestHandler.setConnection(upstream.connection)
  }

  protected def unhandled: PartialHandler[C] = PartialFunction[C#Request,Callback[C#Response]]{
    case other =>
      Callback.successful(processFailure(RecoverableError(other, new UnhandledRequestException(s"Unhandled Request $other"))))
  }

  def serverContext = requestHandler.serverContext

  protected def unhandledError: ErrorHandler[C] 
  def downstream = requestHandler

  private lazy val handler: PartialHandler[C] = requestHandler.handle orElse unhandled
  private lazy val errorHandler: ErrorHandler[C] = requestHandler.onError orElse unhandledError

  protected def processRequest(i: C#Request): Callback[C#Response] = handler(i)

  protected def processFailure(error: ProcessingFailure[C#Request]): C#Response = errorHandler(error)

}

class RequestHandlerException(message: String) extends Exception(message)

abstract class GenRequestHandler[P <: Protocol](val config: ServiceConfig, val serverContext: ServerContext) 
extends DownstreamEvents with HandlerTail with UpstreamEventHandler[ServiceUpstream[P]] {

  def this(context: ServerContext) = this(ServiceConfig.load(context.name), context)

  val server = serverContext.server
  def context = serverContext.context
  implicit val worker = context.worker

  private var _connectionManager: Option[ConnectionManager] = None

  def connection = _connectionManager.getOrElse {
    throw new RequestHandlerException("Cannot access connection before request handler is bound")
  }

  def setConnection(connection: ConnectionManager) {
    _connectionManager = Some(connection)
  }

  implicit val executor   = context.worker.callbackExecutor

  def handle: PartialHandler[P]

  def onError: ErrorHandler[P] = Map()

  def disconnect() {
    connection.disconnect()
  }

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

  protected class ServiceHandler(rh: RequestHandler) 
  extends DSLService[P](rh) {

    def unhandledError = {
      case error => errorMessage(error)
    }

  }

  protected class Generator(context: InitContext) extends HandlerGenerator[RequestHandler](context) {

    def fullHandler = rh => new PipelineHandler(new Controller(new ServiceHandler(rh), provideCodec()), rh)

  }

  abstract class Initializer(context: InitContext) extends Generator(context) with ServiceInitializer[RequestHandler]

  abstract class RequestHandler(ctx: ServerContext, config: ServiceConfig ) extends GenRequestHandler[P](config, ctx){
    def this(ctx: ServerContext) = this(ctx, ServiceConfig.load(ctx.name))
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

  def lift[M[_]](baseClient: Sender[C, M])(implicit async: Async[M]) : T[M]

}

trait ClientFactory[C <: Protocol, M[_], T <: Sender[C,M], E] {

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

trait ServiceClientFactory[P <: Protocol] extends ClientFactory[P, Callback, ServiceClient[P], WorkerRef]

object ServiceClientFactory {
  
  def staticClient[P <: Protocol](name: String, codecProvider: () => Codec.Client[P]) = new ServiceClientFactory[P] {

    def defaultName = name

    def apply(config: ClientConfig)(implicit worker: WorkerRef): ServiceClient[P] = {
      new ServiceClient(codecProvider(), config, worker)
    }
  }

}

class FutureClientFactory[P <: Protocol](base: ServiceClientFactory[P]) extends ClientFactory[P, Future, FutureClient[P], IOSystem] {

  def defaultName = base.defaultName
  
  def apply(config: ClientConfig)(implicit io: IOSystem) = FutureClient.create(config)(io, base)

}

class CodecClientFactory[C <: Protocol, M[_], B <: Sender[C, M], T[M[_]] <: Sender[C,M], E]
(implicit baseFactory: ClientFactory[C, M,B,E], lifter: ClientLifter[C,T], builder: AsyncBuilder[M,E])
extends ClientFactory[C,M,T[M],E] {

  def defaultName = baseFactory.defaultName

  def apply(config: ClientConfig)(implicit env: E): T[M] = {
    apply(baseFactory(config))
  }

  def apply(sender: Sender[C,M])(implicit env: E): T[M] = lifter.lift(sender)(builder.build(env))

}

/**
 * Mixed into protocols to provide simple methods for creating clients.
 */
abstract class ClientFactories[C <: Protocol, T[M[_]] <: Sender[C, M]](implicit lifter: ClientLifter[C,T]) {

  implicit def clientFactory: ServiceClientFactory[C]

  implicit val futureFactory = new FutureClientFactory(clientFactory)

  val client = new CodecClientFactory[C, Callback, ServiceClient[C], T, WorkerRef]

  val futureClient = new CodecClientFactory[C, Future, FutureClient[C], T, IOSystem]

}

trait LiftedClient[C <: Protocol, M[_] ] extends Sender[C,M] {

  def client: Sender[C,M]
  implicit val async: Async[M]

  def send(input: C#Request): M[C#Response] = client.send(input)

  protected def executeAndMap[T](i : C#Request)(f : C#Response => M[T]) = async.flatMap(send(i))(f)

  def disconnect() {
    client.disconnect()
  }

}

class BasicLiftedClient[C <: Protocol, M[_] ](val client: Sender[C,M])(implicit val async: Async[M]) extends LiftedClient[C,M] {

}
