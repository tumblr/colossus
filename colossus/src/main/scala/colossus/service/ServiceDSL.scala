package colossus
package service

import core._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

import java.net.InetSocketAddress
import metrics.MetricAddress

import Codec._


trait CodecDSL {self =>
  type Input
  type Output
}

trait DefaultHandler extends CodecDSL {self => 
  type ConnectionHandler = BasicServiceHandler[self.type]
}

object CodecDSL {

  type PartialHandler[C <: CodecDSL] = PartialFunction[C#Input, Response[C#Output]]

  type HandlerGenerator[C <: CodecDSL] = ConnectionContext[C] => Unit

  type Initializer[C <: CodecDSL] = ServiceContext[C] => HandlerGenerator[C]

  type Receive = PartialFunction[Any, Unit]
}

import CodecDSL._

/**
 * Provide a Codec as well as some convenience functions for usage within in a Service.
 * @tparam C the type of codec this provider will supply
 */
trait CodecProvider[C <: CodecDSL] {
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
  def errorResponse(request: C#Input, reason: Throwable): Completion[C#Output]

  /**
   * Provider of a ConnectionHandler using this Codec
   * @param config ServiceConfig of the Server the ConnectionHandlers will operate in
   * @param worker The Worker to which the Connection is bound
   * @param ex ExecutionContext
   * @return Handler
   */
  def provideHandler(config: ServiceConfig, worker: WorkerRef)(implicit ex: ExecutionContext): DSLHandler[C] = {
    new BasicServiceHandler[C](config,worker,this)
  }

  /**
   * Provider of a Delegator using this Codec
   * @param func Function which provides a Delegator for this Service
   * @param server Server which this Delegator will operate in
   * @param worker Worker which the Delegator will be bound
   * @param provider The codecProvider
   * @param requestTimeout RequestTimeout
   * @return Delegator
   */
  def provideDelegator(func: Initializer[C], server: ServerRef, worker: WorkerRef, provider: CodecProvider[C], config: ServiceConfig) = {
    new BasicServiceDelegator(func, server, worker, provider, config)
  }
}

trait ClientCodecProvider[C <: CodecDSL] {
  def name: String
  def clientCodec(): ClientCodec[C#Input, C#Output]
}

trait ConnectionContext[C <: CodecDSL] {
  def become(p: PartialHandler[C])
  def process(f: C#Input => Response[C#Output]){
    become{case all => f(all)}
  }
  def disconnect()

  implicit val callbackExecutor: CallbackExecutor
}

trait ServiceContext[C <: CodecDSL] {
  def worker: WorkerRef
  def handle(p: HandlerGenerator[C]) = p //this method is just for clarity
  //delegator message handling
  def receive(receiver: Receive)

  def clientFor[D <: CodecDSL](config: ClientConfig)(implicit provider: ClientCodecProvider[D]): ServiceClient[D#Input, D#Output] = {
    ServiceClient(provider.clientCodec(), config, worker)
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
    
}

abstract class DSLDelegator[C <: CodecDSL](server : ServerRef, worker : WorkerRef) extends Delegator(server, worker) with ServiceContext[C]

class BasicServiceDelegator[C <: CodecDSL](func: Initializer[C], server: ServerRef, worker: WorkerRef, provider: CodecProvider[C], config: ServiceConfig)
  extends DSLDelegator[C](server, worker){

  //note, this needs to be setup before func is called
  private var currentReceiver: Receive = {case _ => ()}
  
  val handlerInitializer: HandlerGenerator[C] = func(this)

  def acceptNewConnection: Option[ConnectionHandler] = {
    val handler: DSLHandler[C] = provider.provideHandler(config, worker)
    handlerInitializer(handler)
    Some(handler)
  }

  def receive(r: Receive) {
    currentReceiver = r
  }

  //TODO: This should be settable, or the currentReceiver needs to be settable, that way,
  //in the Initializer[T] function that is invoked, custom behavior can be supplied to this delegator
  override def handleMessage: Receive = currentReceiver
}

trait DSLHandler[C <: CodecDSL] extends ServiceServer[C#Input, C#Output] with ConnectionContext[C]

class UnhandledRequestException(message: String) extends Exception(message)

class BasicServiceHandler[C <: CodecDSL]
  (config: ServiceConfig, worker: WorkerRef, provider: CodecProvider[C]) 
  (implicit ex: ExecutionContext)
  extends ServiceServer[C#Input, C#Output](provider.provideCodec(), config, worker) 
  with DSLHandler[C] {

  protected def unhandled: PartialHandler[C] = PartialFunction[C#Input,Response[C#Output]]{
    case other => respond(provider.errorResponse(other, new UnhandledRequestException(s"Unhandled request $other")))
  }
  
  private var currentHandler: PartialHandler[C] = unhandled

  def become(handler: PartialHandler[C]) {
    currentHandler = handler
  }

  protected def fullHandler: PartialFunction[C#Input, Response[C#Output]] = currentHandler orElse unhandled
  
  protected def processRequest(i: C#Input): Response[C#Output] = fullHandler(i)
  
  protected def processFailure(request: C#Input, reason: Throwable): Completion[C#Output] = provider.errorResponse(request, reason)

}


/**
 * The Service object is an entry point into the the Service DSL which provide some convenience functions for quickly
 * creating a Server serving responses to requests utilizing a Codec(ie: memcached, http, redis, etc).
 *
 * One thing to always keep in mind is that code inside the Service.serve is placed inside a Delegator and ConnectionHandler,
 * which means that it directly runs inside of a Worker and its SelectLoop.
 * Be VERY mindful of the code that you place in here, as if there is any blocking code it will block the Worker.  Not good.
 *
 *
 * An example with full typing in place to illustrate :
 *
 * {{{
 * import colossus.protocols.http._  //imports an implicit codec
 *
 * implicit val ioSystem : IOSystem = myBootStrappingFunction()
 *
 * Service.serve[Http]("my-app", 9090) { context : ServiceContext[Http] =>
 *
 *   //everything in this block happens on delegator initialization, which is on application startup.  One time only.
 *
 *   context.handle { connection : ConnectionContext[Http] => {
 *       //This block is for connectionHandler initialization. Happens in the event loop.  Don't block.
 *       //Note, that a connection can handle multiple requests.
 *       connection.become {
 *         //this partial function is what "handles" a request.  Again.. don't block.
 *         case req @ Get on Text("test") => future(req.ok(someService.getDataFromStore()))
 *         case req @ Get on Text("path") => req.ok("path")
 *       }
 *     }
 *   }
 * }
 *}}}
 *
 *
 */
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
  def serve[T <: CodecDSL]
  (serverSettings: ServerSettings, serviceConfig: ServiceConfig)
  (handler: Initializer[T])
  (implicit system: IOSystem, provider: CodecProvider[T]): ServerRef = {
    val serverConfig = ServerConfig(
      name = serviceConfig.name,
      settings = serverSettings,
      delegatorFactory = (s,w) => provider.provideDelegator(handler, s, w, provider, serviceConfig)
    )
    Server(serverConfig)
  }

  /** Quick-start a service, using default settings 
   *
   * @param name The name of the service
   * @param port The port to bind the server to
   */
  def serve[T <: CodecDSL]
  (name: String, port: Int, requestTimeout: Duration = 100.milliseconds)
  (handler: Initializer[T])
  (implicit system: IOSystem, provider: CodecProvider[T]): ServerRef = { 
    serve[T](ServerSettings(port), ServiceConfig(name = name, requestTimeout = requestTimeout))(handler)
  }

  /** Start a simple, stateless service
   *
   * Unlike `Service.serve`, there is no room for per-worker or per-connection
   * initialization.  Useful when starting simple services or testing
   *
   * @param name Name of this Service
   * @param port Port on which this Server will accept connections
   * @param handler 
   * @param system The IOSystem to which this Server will belong
   * @param provider CodecProvider
   * @tparam T the type of codec this service uses
   * @return
   */
  def become[T <: CodecDSL]
  (name: String, port: Int, requestTimeout: Duration = 100.milliseconds)
  (handler: PartialHandler[T])
  (implicit system: IOSystem, provider: CodecProvider[T]): ServerRef = {
    serve[T](name, port, requestTimeout){context =>
      context.handle{connection =>
        connection.become(handler)
      }
    }
  }
}
