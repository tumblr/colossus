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
  
  private lazy val handler: PartialHandler[C] = handle orElse unhandled
  private lazy val errorHandler: ErrorHandler[C] = onError orElse unhandledError

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


/**
 * This has to be implemented per codec in order to lift generic Sender traits to a type-specific trait
 *
 * For example this is how we go from ServiceClient[HttpRequest, HttpResponse] to HttpClient[Callback]
 */
trait ClientLifter[C <: Protocol, T[M[_]] <: Sender[C,M]] {

  def lift[M[_]](baseClient: Sender[C, M])(implicit async: Async[M]) : T[M]

}

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


  implicit def serviceClientFactory[C <: Protocol] = new ClientFactory[C, Callback, ServiceClient[C], WorkerRef] {
    
    def apply(config: ClientConfig)(implicit provider: ClientCodecProvider[C], worker: WorkerRef): ServiceClient[C] = {
      new ServiceClient(provider.clientCodec(), config, worker.generateContext())
    }

  }

  implicit def futureClientFactory[C <: Protocol] = new ClientFactory[C, Future, FutureClient[C], IOSystem] {
    
    def apply(config: ClientConfig)(implicit provider: ClientCodecProvider[C], io: IOSystem) = {
      AsyncServiceClient.create(config)(io, provider)
    }

  }

}

class CodecClientFactory[C <: Protocol, M[_], B <: Sender[C, M], T[M[_]] <: Sender[C,M], E]
(implicit baseFactory: ClientFactory[C, M,B,E], lifter: ClientLifter[C,T], builder: AsyncBuilder[M,E])
extends ClientFactory[C,M,T[M],E] {

  def apply(config: ClientConfig)(implicit provider: ClientCodecProvider[C], env: E): T[M] =  {
    apply(baseFactory(config))
  }

  def apply(sender: Sender[C,M])(implicit env: E): T[M] = lifter.lift(sender)(builder.build(env))

}

/**
 * Mixed into protocols to provide simple methods for creating clients.
 */
class ClientFactories[C <: Protocol, T[M[_]] <: Sender[C, M]](implicit lifter: ClientLifter[C, T]){

  import ClientFactory._

  
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
