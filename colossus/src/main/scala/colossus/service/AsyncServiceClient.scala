package colossus
package service

import core._

import akka.actor._
import akka.util.Timeout
import akka.pattern.ask
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

/**
 * This correctly routes messages to the right worker and handler
 */
class ClientProxy(config: ClientConfig, system: IOSystem, handlerFactory: ActorRef => WorkerRef => ClientConnectionHandler) extends Actor with ActorLogging  with Stash {
  import WorkerCommand._
  import ConnectionEvent._
  import config._

  override def preStart() {
    system.workerManager ! IOCommand.Connect(address, handlerFactory(self))
    context.become(binding)
  }

  def receive = binding

  def binding: Receive = {
    case Bound(id) => {
      context.become(proxy(id, sender))
      unstashAll()
    }
    case x => stash()

  }


  def proxy(connectionId: Long, worker: ActorRef): Receive = {
    case Connected => {} //we ignore this because there's nothing to do with it.  Maybe add a callback in the future
    case AsyncServiceClient.Disconnect => self ! PoisonPill //the worker is watching us and will close the underlying connection
    case x => worker ! Message(connectionId, x)
  }

}

trait AsyncServiceClient[I,O] extends SharedClient[I,O] {
  def connectionStatus: Future[ConnectionStatus]
  def disconnect()
}

object AsyncServiceClient {

  sealed trait ClientCommand

  case object Disconnect extends ClientCommand
  case class GetConnectionStatus(promise: Promise[ConnectionStatus] = Promise()) extends ClientCommand

  def apply[Request, Response](config: ClientConfig, codec: Codec.ClientCodec[Request, Response])(implicit io: IOSystem): AsyncServiceClient[Request,Response] = {
    val gen = new AsyncHandlerGenerator(config, codec)
    val actor = io.actorSystem.actorOf(Props(classOf[ClientProxy], config, io, gen.handlerFactory))
    gen.client(actor)
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
  import ConnectionEvent._

  case class PackagedRequest(request: I, response: Promise[O])

  /**
   * this is used to communicate with an external actor being used as a service client.
   */
  class AsyncHandler(
    config: ClientConfig,
    worker: WorkerRef,
    val caller: ActorRef
  ) extends ServiceClient[I,O](codec, config, worker) with WatchedHandler {
    implicit val sender = worker.worker
    val watchedActor = caller

    override def bound(id: Long, worker: WorkerRef) {
      super.bound(id, worker)
      caller.!( ConnectionEvent.Bound(id))(worker.worker)
    }

    override def receivedMessage(message: Any, sender: ActorRef) {
      message match {
        case PackagedRequest(request, promise) => {
          println("GOT REQUEST")
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

  def client(proxy: ActorRef): AsyncServiceClient[I,O] = new AsyncServiceClient[I,O] {
    def send(request: I): Future[O] = {
      val promise = Promise[O]()
      proxy ! PackagedRequest(request, promise)
      promise.future
    }

    def disconnect() {
      proxy ! PoisonPill
    }

    //TODO: when the user manually calls disconnect, this future never
    //completes.  This isn't terrible but we should think of something more
    //meaningful
    def connectionStatus: Future[ConnectionStatus] = {
      val s = AsyncServiceClient.GetConnectionStatus()
      proxy ! s
      s.promise.future
    }
  }

  val handlerFactory: ActorRef => WorkerRef => ConnectionHandler = caller => worker => new AsyncHandler(config, worker, caller)

}
