package colossus
package service

import core._

import akka.actor._
import akka.util.Timeout
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.language.higherKinds


class ProxyWatchdog(proxy: ActorRef, signal: AtomicBoolean) extends Actor {

  override def preStart() {
    context.watch(proxy)
  }

  def receive = {
    case Terminated(ref) => {
      signal.set(true)
      self ! PoisonPill
    }
  }
}

trait Client[P <: Protocol, M[_]] extends Sender[P, M] {
  def connectionStatus: M[ConnectionStatus]
  def disconnect()

}

trait CallbackClient[P <: Protocol] extends Client[P, Callback]
trait FutureClient[C <: Protocol] extends Client[C, Future]

object FutureClient {

  sealed trait ClientCommand

  type BaseFactory[P <: Protocol] = ClientFactory[P, Callback, Client[P,Callback], WorkerRef]

  case class GetConnectionStatus(promise: Promise[ConnectionStatus] = Promise()) extends ClientCommand

  def apply[C <: Protocol](config: ClientConfig)(implicit io: IOSystem, base: BaseFactory[C]): FutureClient[C] = create(config)(io, base)

  def create[C <: Protocol](config: ClientConfig)(implicit io: IOSystem, base: BaseFactory[C]): FutureClient[C] = {
    val gen = new AsyncHandlerGenerator(config, base)
    gen.client
  }

}

/**
 * So we need to take a type-parameterized request object, package it into a
 * monomorphic case class to send to the worker, and have the handler that
 * receives that object able to pattern match out the parameterized object, all
 * without using reflection.  We can do that with some nifty path-dependant
 * types
 */
class AsyncHandlerGenerator[C <: Protocol](config: ClientConfig, base: FutureClient.BaseFactory[C])(implicit sys: IOSystem) {

  type I = C#Request
  type O = C#Response

  case class PackagedRequest(request: I, response: Promise[O])

  class ClientWrapper(val context: Context) extends WorkerItem with ProxyActor {

    val client = base(config)(worker)

    def receive = {
      case PackagedRequest(request, promise) => {
        client.send(request).execute(promise.complete)
      }
      case FutureClient.GetConnectionStatus(promise) => {
        import scala.concurrent.ExecutionContext.Implicits.global
        promise.completeWith(client.connectionStatus.toFuture)
      }

    }
    override def onUnbind() {
      super.onUnbind()
      client.disconnect()
    }
  }

  implicit val timeout = Timeout(100.milliseconds)

  protected val proxy = sys.bindWithProxy(new ClientWrapper(_))

  //the canary is used to determine when it's no longer ok to try sending
  //requests to the proxy.  This is set to true if the user calls disconnect or
  //if the proxy actor is killed (which is detected by the watchdog).  This
  //provides a reasonable attempt to prevent requests from being dropped and
  //never completing their promise, though it's not a gaurantee since it's
  //possible for a client to kill itself before it receives a request that had
  //already been sent.
  protected val canary = new AtomicBoolean(false)

  protected val watchdog = sys.actorSystem.actorOf(Props(classOf[ProxyWatchdog], proxy, canary))

  val client = new FutureClient[C]{
    def send(request: I): Future[O] = {
      if (canary.get()) {
        Future.failed(new NotConnectedException("Connection Closed"))
      } else {
        val promise = Promise[O]()
        proxy ! PackagedRequest(request, promise)
        promise.future
      }
    }

    def disconnect() {
      if (!canary.get()) {
        canary.set(true)
        proxy ! PoisonPill
      }
    }

    def connectionStatus: Future[ConnectionStatus] = {
      if (canary.get()) {
        Future.successful(ConnectionStatus.NotConnected)
      } else {
        val s = FutureClient.GetConnectionStatus()
        proxy ! s
        s.promise.future
      }
    }

    val clientConfig = config
  }

}
