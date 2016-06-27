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
import controller.StaticCodec

import Codec._

trait Encoding {
  type Input
  type Output
}
object Encoding {
  type Apply[I,O] = Encoding { type Input = I; type Output = O }
}

trait Protocol {self =>
  type Request
  type Response

  //deprecated
  type Input = Request
  //deprecated
  type Output = Response

  trait ServerEncoding extends Encoding {
    type Input = Request
    type Output = Response
  }

  trait ClientEncoding extends Encoding {
    type Input = Response
    type Output = Request
  }


}

object Protocol {

  type PartialHandler[C <: Protocol] = PartialFunction[C#Input, Callback[C#Output]]

  type Receive = PartialFunction[Any, Unit]

  type ErrorHandler[C <: Protocol] = PartialFunction[ProcessingFailure[C#Input], C#Output]

  type ParseErrorHandler[C <: Protocol] = PartialFunction[Throwable, C#Output]
}

import Protocol._

class UnhandledRequestException(message: String) extends Exception(message)
class ReceiveException(message: String) extends Exception(message)

trait DSLService[C <: Protocol] extends ServiceServer[C] with ConnectionManager{ 

  def requestHandler: GenRequestHandler[C]

  protected def unhandled: PartialHandler[C] = PartialFunction[C#Input,Callback[C#Output]]{
    case other =>
      Callback.successful(processFailure(RecoverableError(other, new UnhandledRequestException(s"Unhandled Request $other"))))
  }

  protected def unhandledError: ErrorHandler[C] 

  private lazy val handler: PartialHandler[C] = requestHandler.handle orElse unhandled
  private lazy val errorHandler: ErrorHandler[C] = requestHandler.onError orElse unhandledError

  protected def processRequest(i: C#Input): Callback[C#Output] = handler(i)

  protected def processFailure(error: ProcessingFailure[C#Input]): C#Output = errorHandler(error)

}

/**
 * This needs to be mixed with Controller, ServiceServer, and DSLService
 * sub-traits to work (right now there's only one implementation of each but
 * that may change
 *
 */
abstract class BasicServiceHandler[P <: Protocol](val requestHandler : GenRequestHandler[P]) 
extends {
  val serverContext = requestHandler.context
  val config        = requestHandler.config

} with DSLService[P] {

  override def onBind() {
    requestHandler.onBind(this)
  }

}

class RequestHandlerException(message: String) extends Exception(message)

abstract class GenRequestHandler[P <: Protocol](val config: ServiceConfig, val context: ServerContext) {

  def this(context: ServerContext) = this(ServiceConfig.load(context.name), context)

  val server = context.server
  implicit val worker = context.context.worker

  private var _connectionManager: Option[ConnectionManager] = None

  def connection = _connectionManager.getOrElse {
    throw new RequestHandlerException("Cannot access connection before request handler is bound")
  }

  def onBind(connection: ConnectionManager) {
    _connectionManager = Some(connection)
  }

  implicit val executor   = context.context.worker.callbackExecutor

  def handle: PartialHandler[P]

  def onError: ErrorHandler[P] = Map()

  def disconnect() {
    connection.disconnect()
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
  
  def staticClient[P <: Protocol](name: String, codecProvider: () => StaticCodec.Client[P]) = new ServiceClientFactory[P] {

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

  //implicit def lifter: ClientLifter[C,T]

  implicit def clientFactory: ServiceClientFactory[C]

  implicit val futureFactory = new FutureClientFactory(clientFactory)

  val client = new CodecClientFactory[C, Callback, ServiceClient[C], T, WorkerRef]

  val futureClient = new CodecClientFactory[C, Future, FutureClient[C], T, IOSystem]

}

class LiftedClient[C <: Protocol, M[_] ](val client: Sender[C,M])(implicit val async: Async[M]) extends Sender[C,M] {

  def send(input: C#Input): M[C#Output] = client.send(input)

  protected def executeAndMap[T](i : C#Input)(f : C#Output => M[T]) = async.flatMap(send(i))(f)

  def disconnect() {
    client.disconnect()
  }
}
