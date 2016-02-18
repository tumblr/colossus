package colossus
package service

import core._

import akka.actor.ActorRef
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

import java.net.InetSocketAddress
import metrics.MetricAddress

import Codec._


trait CodecDSL {self =>
  type Input
  type Output
}

object CodecDSL {

  type PartialHandler[C <: CodecDSL] = PartialFunction[C#Input, Callback[C#Output]]

  type Receive = PartialFunction[Any, Unit]

  type ErrorHandler[C <: CodecDSL] = PartialFunction[(C#Input, Throwable), C#Output]
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
  def errorResponse(request: C#Input, reason: Throwable): C#Output

}

trait ClientCodecProvider[C <: CodecDSL] {
  def name: String
  def clientCodec(): ClientCodec[C#Input, C#Output]
}




class UnhandledRequestException(message: String) extends Exception(message)
class ReceiveException(message: String) extends Exception(message)

abstract class Service[C <: CodecDSL]
(val config: ServiceConfig, context: Context)
(implicit val provider: CodecProvider[C]) 
extends ServiceServer[C#Input, C#Output](provider.provideCodec(), config, context) {

  implicit val executor = context.worker.callbackExecutor

  //TODO: fix it, this should pull from some kind of default config
  def this(context: Context)(implicit provider: CodecProvider[C]) = this(ServiceConfig("FIX THIS", Duration.Inf), context)(provider)

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
   /*
  def serve[T <: CodecDSL]
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
  def basic[T <: CodecDSL]
  (name: String, port: Int, requestTimeout: Duration = 100.milliseconds)(userHandler: PartialHandler[T])
  (implicit system: IOSystem, provider: CodecProvider[T]): ServerRef = { 
    class BasicService(context: Context) extends Service(ServiceConfig(name = name, requestTimeout = requestTimeout), context) {
      def handle = userHandler
    }
    Server.basic(name, port)(context => new BasicService(context))
  }

}
