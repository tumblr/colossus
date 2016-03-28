package colossus
package service

import core._

import akka.actor.ActorRef
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds

import java.net.InetSocketAddress
import metrics.MetricAddress

import Codec._


//TODO : Rename to Protocol
trait Protocol {self =>
  type Input
  type Output

}

object Protocol {

  type PartialHandler[C <: Protocol] = PartialFunction[C#Input, Callback[C#Output]]

  type Receive = PartialFunction[Any, Unit]

  type ErrorHandler[C <: Protocol] = PartialFunction[(C#Input, Throwable), C#Output]
}

import Protocol._

/**
 * Provide a Codec as well as some convenience functions for usage within in a Service.
 * @tparam C the type of codec this provider will supply
 */
trait CodecProvider[C <: Protocol] {
  /**
   * The Codec which will be used.
   * @return
   */
  def provideCodec(): ServerCodec[C#Input, C#Output]

  /**
   * Basic error response
   * @param request Request that caused the error
   * @param reason The resulting failure
   * @return A response which represents the failure encoded with the Codec
   */
  def errorResponse(request: C#Input, reason: Throwable): C#Output

}

trait ClientCodecProvider[C <: Protocol] {
  def name: String
  def clientCodec(): ClientCodec[C#Input, C#Output]
}


/**
 * Mixed into base protocol objects to provide a default codec provider
 */
trait ServerDefaults[C <: Protocol] {
  implicit def serverDefaults : CodecProvider[C]

}

/**
 * Mixed into base protocol objects to provide a default client codec provider
 */
trait ClientDefaults[C <: Protocol] {
  implicit def clientDefaults : ClientCodecProvider[C]
}

trait CodecDefaults[C <: Protocol] extends ServerDefaults[C] with ClientDefaults[C]


class UnhandledRequestException(message: String) extends Exception(message)
class ReceiveException(message: String) extends Exception(message)

abstract class Service[C <: Protocol]
(codec: ServerCodec[C#Input, C#Output], config: ServiceConfig, srv: ServerContext)(implicit provider: CodecProvider[C])
extends ServiceServer[C#Input, C#Output](codec, config, srv) {

  implicit val executor   = context.worker.callbackExecutor

  def this(config: ServiceConfig, context: ServerContext)(implicit provider: CodecProvider[C]) = this(provider.provideCodec, config, context)

  def this(context: ServerContext)(implicit provider: CodecProvider[C]) = this(ServiceConfig(), context)(provider)

  protected def unhandled: PartialHandler[C] = PartialFunction[C#Input,Callback[C#Output]]{
    case other => Callback.successful(processFailure(other, new UnhandledRequestException(s"Unhandled Request $other")))
  }

  protected def unhandledReceive: Receive = {
    case _ => {}
  }

  protected def unhandledError: ErrorHandler[C] = {
    case (request, reason) => provider.errorResponse(request, reason)
  }

  private var currentSender: Option[ActorRef] = None

  def sender(): ActorRef = currentSender.getOrElse {
    throw new ReceiveException("No sender")
  }
  
  private val handler: PartialHandler[C] = handle orElse unhandled
  private val errorHandler: ErrorHandler[C] = onError orElse unhandledError

  def receivedMessage(message: Any, sender: ActorRef) {
    currentSender = Some(sender)
    receive(message)
    currentSender = None
  }
    
  protected def processRequest(i: C#Input): Callback[C#Output] = handler(i)
  
  protected def processFailure(request: C#Input, reason: Throwable): C#Output = errorHandler((request, reason))


  def handle: PartialHandler[C]

  def onError: ErrorHandler[C] = Map()

  def receive: Receive = Map()

}



object Service {
  /** Start a service with worker and connection initialization
   *
   * The basic structure of a service using this method is:{{{
   Service.serve[Http]{ workerContext =>
     //worker initialization
     workerContext.handle { connectionContext =>
       //connection initialization
       connection.become {
         case ...
       }
     }
   }
   }}}
   *
   * @param serverSettings Settings to provide the underlying server
   * @param serviceConfig Config for the service
   * @param handler The worker initializer to use for the service
   * @tparam T The codec to use, eg Http, Redis
   * @return A [[ServerRef]] for the server.
   */
   /*
  def serve[T <: Protocol]
  (serverSettings: ServerSettings, serviceConfig: ServiceConfig[T#Input, T#Output])
  (handler: Initializer[T])
  (implicit system: IOSystem, provider: CodecProvider[T]): ServerRef = {
    val serverConfig = ServerConfig(
      name = serviceConfig.name,
      settings = serverSettings,
      delegatorFactory = (s,w) => provider.provideDelegator(handler, s, w, provider, serviceConfig)
    )
    Server(serverConfig)
  }
  */

  /** Quick-start a service, using default settings 
   *
   * @param name The name of the service
   * @param port The port to bind the server to
   */
  def basic[T <: Protocol]
  (name: String, port: Int, requestTimeout: Duration = 100.milliseconds)(userHandler: PartialHandler[T])
  (implicit system: IOSystem, provider: CodecProvider[T]): ServerRef = { 
    class BasicService(context: ServerContext) extends Service(ServiceConfig(requestTimeout = requestTimeout), context) {
      def handle = userHandler
    }
    Server.basic(name, port)(context => new BasicService(context))
  }

}


trait CodecClient[C <: Protocol] extends ServiceClient[C#Input, C#Output] with Sender[C, Callback] 

/**
 * This has to be implemented per codec per sender type (ServiceClient, AsyncServiceClient, etc)
 *
 * For example this is how we go from ServiceClient[HttpRequest, HttpResponse] to HttpClient[Callback]
 */
trait ClientLifter[C <: Protocol, M[_],B <: Sender[C,M], T <: Sender[C,M], E] {

  def lift(baseClient: B)(implicit environment: E) : T

}

trait CallbackLifter[C <: Protocol, B <: Sender[C, Callback], T <: Sender[C, Callback]] extends ClientLifter[C, Callback, B, T, WorkerRef]
trait FutureLifter[C <: Protocol, B <: Sender[C, Future], T <: Sender[C, Future]] extends ClientLifter[C, Future, B, T, IOSystem]

trait ServiceClientLifter[C <: Protocol, T <: Sender[C, Callback]] extends CallbackLifter[C, CodecClient[C], T]
trait FutureClientLifter[C <: Protocol, T <: Sender[C, Future]] extends FutureLifter[C, FutureClient[C], T]

trait ClientFactory[C <: Protocol, M[_], T <: Sender[C,M], E] {
  

  def apply(config: ClientConfig)(implicit provider: ClientCodecProvider[C], env: E): T

  def apply(host: String, port: Int, requestTimeout: Duration = 1.second)(implicit provider: ClientCodecProvider[C], env: E): T = {
    apply(new InetSocketAddress(host, port), requestTimeout)
  }

  def apply (address: InetSocketAddress, requestTimeout: Duration) (implicit provider: ClientCodecProvider[C], env: E): T = {
    val config = ClientConfig(
      address = address,
      requestTimeout = requestTimeout,
      name = MetricAddress.Root / provider.name
    )
    apply(config)
  }

}


object ClientFactory {


  implicit def serviceClientFactory[C <: Protocol] = new ClientFactory[C, Callback, CodecClient[C], WorkerRef] {
    
    def apply(config: ClientConfig)(implicit provider: ClientCodecProvider[C], worker: WorkerRef): CodecClient[C] = {
      new ServiceClient(provider.clientCodec(), config, worker.generateContext()) with CodecClient[C]
    }

  }

  implicit def futureClientFactory[C <: Protocol] = new ClientFactory[C, Future, FutureClient[C], IOSystem] {
    
    def apply(config: ClientConfig)(implicit provider: ClientCodecProvider[C], io: IOSystem) = {
      AsyncServiceClient(config)(io, provider)
    }

  }

}

class CodecClientFactory[C <: Protocol, M[_], B <: Sender[C, M], T <: Sender[C,M], E]
(implicit baseFactory: ClientFactory[C, M,B,E], lifter: ClientLifter[C,M,B,T,E])
extends ClientFactory[C,M,T,E] {

  def apply(config: ClientConfig)(implicit provider: ClientCodecProvider[C], env: E): T =  lifter.lift(baseFactory(config))

}

/**
 * Mixed into protocols to provide simple methods for creating clients.
 */
class ClientFactories[C <: Protocol, T[M[_]] <: Sender[C, M]] 
(implicit serviceLifter: CallbackLifter[C, CodecClient[C], T[Callback]], futureLifter: FutureLifter[C, FutureClient[C], T[Future]]){

  import ClientFactory._

  
  val client = new CodecClientFactory[C, Callback, CodecClient[C], T[Callback], WorkerRef]

  val futureClient = new CodecClientFactory[C, Future, FutureClient[C], T[Future], IOSystem]

}

//TODO : These two classes can be combined if add an environment type as we do with the factories

/**
 * This is used by protocols and mixed in with a protocol-specific trait to
 * provide a callback-based client
 */
class LiftedCallbackClient[C <: Protocol](val client : Sender[C, Callback]) 
  extends CallbackResponseAdapter[C]


/**
 * This is used by protocols and mixed in with a protocol-specific trait to
 * provide a future-based client
 */
class LiftedFutureClient[C <: Protocol](val client : Sender[C, Future])(implicit val executionContext : ExecutionContext)
  extends FutureResponseAdapter[C]

