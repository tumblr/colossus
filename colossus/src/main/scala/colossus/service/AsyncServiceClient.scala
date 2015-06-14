package colossus
package service

import core._

import akka.actor._
import akka.util.{ByteString, Timeout}
import java.net.InetSocketAddress
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

sealed trait ConnectionEvent
sealed trait ClientConnectionEvent extends ConnectionEvent
object ConnectionEvent {
  case class ReceivedData(data: ByteString) extends ConnectionEvent
  case object ReadyForData extends ConnectionEvent
  //maybe include an id or something
  case class WriteAck(status: WriteStatus) extends ConnectionEvent
  case class ConnectionTerminated(cause : DisconnectCause) extends ConnectionEvent

  //for server connections, Connected is send immediately after Bound.  For
  //clients, the messages are more semantic, Bound is sent immediately while
  //connected only when the connection is fully established
  case class Bound(id: Long) extends ConnectionEvent

  case object Connected extends ConnectionEvent
  //for client connections
  case object ConnectionFailed extends ClientConnectionEvent

  case object Unbound extends ConnectionEvent
}

/**
 * This correctly routes messages to the right worker and handler
 */
class ClientProxy(config: ClientConfig, system: IOSystem, handlerFactory: ActorRef => WorkerRef => ClientConnectionHandler) extends Actor with ActorLogging  with Stash {
  import WorkerCommand._
  import ConnectionEvent._

  override def preStart() {
    system.workerManager ! IOCommand.BindWorkerItem(handlerFactory(self))
    context.become(binding)
  }

  def receive = binding

  def binding: Receive = {
    case Bound(id) => {
      println(s"service client bound $id")
      context.become(proxy(id, sender))
      unstashAll()
    }
    case x => stash()

  }


  def proxy(connectionId: Long, worker: ActorRef): Receive = {
    case Bound(wat) => {
      println(s"RECEIVED BOUND AGAIN! $connectionId vs $wat")
    }
    case Unbound => context.become(dead)
    case Connected => {} //we ignore this because there's nothing to do with it.  Maybe add a callback in the future
    case AsyncServiceClient.Disconnect => {
      worker ! Disconnect(connectionId)
      context.become(dying)
    }
    case m: Worker.MessageDeliveryFailed => {
      println(s"received failed message delivery $m")
    }
    case x => worker ! Message(connectionId, x)
  }

  def dying: Receive = {
    case Unbound => context.become(dead)
    case AsyncServiceClient.GetConnectionStatus(promise) => {
      promise.success(ConnectionStatus.Connected)  //we have to fulfill this since it will never reach the handler
    }
  }

  def dead: Receive = {
    case AsyncServiceClient.GetConnectionStatus(promise) => {
      promise.success(ConnectionStatus.NotConnected)
    }
  }

}

trait AsyncServiceClient[I,O]  {
  
  def send(request: I): Future[O]

  def connectionStatus: Future[ConnectionStatus]
  def disconnect()

  /** Kills the proxy actor and terminates the underlying connection.  
   * This is different from disconnect because disconnect will not kill the
   * proxy actor (useful for verifying that a connection has terminated.  Once
   * this method has been called, any future calls to connectionStatus will
   * return a Future that never completes
   *
   * maybe there's a better way to do this, but AsyncServiceClient isn't used
   * much outside of tests, so we need some more use cases
   */
  def kill()

  def clientConfig : ClientConfig
}

object AsyncServiceClient {

  sealed trait ClientCommand

  case object Disconnect extends ClientCommand
  case class GetConnectionStatus(promise: Promise[ConnectionStatus] = Promise()) extends ClientCommand

  def apply[Request, Response](config: ClientConfig, codec: Codec.ClientCodec[Request, Response])(implicit io: IOSystem): AsyncServiceClient[Request,Response] = {
    val gen = new AsyncHandlerGenerator(config, codec)
    val actor = io.actorSystem.actorOf(Props(classOf[ClientProxy], config, io, gen.handlerFactory))
    gen.client(actor, config)
  }

  def apply[C <: CodecDSL](host: String, port: Int, requestTimeout: Duration = 100.milliseconds)(implicit io: IOSystem, provider: ClientCodecProvider[C]): AsyncServiceClient[C#Input, C#Output] = {
    val config = ClientConfig(
      name = provider.name,
      address = new InetSocketAddress(host, port),
      requestTimeout = requestTimeout
    )
    AsyncServiceClient[C](config)
  }

  def apply[C <: CodecDSL](config: ClientConfig)(implicit io: IOSystem, provider: ClientCodecProvider[C]): AsyncServiceClient[C#Input, C#Output] = {
    AsyncServiceClient(config, provider.clientCodec())
  }
}

/**
 * So we need to take a type-parameterized request object, package it into a
 * monomorphic case class to send to the worker, and have the handler that
 * receives that object able to pattern match out the parameterized object, all
 * without using reflection.  We can do that with some nifty path-dependant
 * types
 */
class AsyncHandlerGenerator[I,O](config: ClientConfig, codec: Codec[I,O]) {

  case class PackagedRequest(request: I, response: Promise[O])

  /**
   * this is used to communicate with an external actor being used as a service client.
   */
  class AsyncHandler(
    config: ClientConfig,
    val caller: ActorRef,
    worker: WorkerRef
  ) extends ServiceClient[I,O](codec, config, worker) with WatchedHandler {
    val watchedActor = caller

    override def onBind() {
      super.onBind()
      caller.!(ConnectionEvent.Bound(id.get))(boundWorker.get.worker)
    }

    override def onUnbind() {
      super.onUnbind()
      caller.!(ConnectionEvent.Unbound)()
    }

    override def receivedMessage(message: Any, sender: ActorRef) {
      message match {
        case PackagedRequest(request, promise) => {
          send(request).execute(promise.complete)
        }
        case AsyncServiceClient.GetConnectionStatus(promise) => {
          promise.success(connectionStatus)
        }
        case other => super.receivedMessage(message, sender)
      }
    }
  }

  implicit val timeout = Timeout(100.milliseconds)

  def client(proxy: ActorRef, cConfig : ClientConfig): AsyncServiceClient[I,O] = new AsyncServiceClient[I,O] {
    def send(request: I): Future[O] = {
      val promise = Promise[O]()
      proxy ! PackagedRequest(request, promise)
      promise.future
    }

    def disconnect() {
      proxy ! AsyncServiceClient.Disconnect
    }

    def kill() {
      proxy ! PoisonPill
    }

    //TODO: when the user manually calls disconnect, this future never
    //completes.  This isn't terrible but we should think of something more
    //meaningful
    def connectionStatus: Future[ConnectionStatus] = {
      import scala.concurrent.ExecutionContext.Implicits.global
      val s = AsyncServiceClient.GetConnectionStatus()
      proxy ! s
      s.promise.future
    }

    val clientConfig = cConfig
  }

  val handlerFactory: ActorRef => WorkerRef =>  ConnectionHandler = caller => worker => new AsyncHandler(config, caller, worker)

}
