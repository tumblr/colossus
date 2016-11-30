package colossus
package service

import core._

import akka.actor._
import akka.util.{ByteString, Timeout}
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._


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

trait FutureClient[C <: Protocol] extends Sender[C, Future] {
  def connectionStatus: Future[ConnectionStatus]
  def disconnect()
  def clientConfig : ClientConfig
}

trait FutureClientOps {

  sealed trait ClientCommand

  case class GetConnectionStatus(promise: Promise[ConnectionStatus] = Promise()) extends ClientCommand

  def create[C <: Protocol](config: ClientConfig)(implicit io: IOSystem, provider: ClientCodecProvider[C]): FutureClient[C] = {
    val gen = new AsyncHandlerGenerator(config, provider.clientCodec())
    gen.client
  }

  def apply[C <: Protocol] = ClientFactory.futureClientFactory[C]
}
object FutureClient extends FutureClientOps

//TODO: remove in 0.9.x
@deprecated("Use FutureClient Instead", "0.8.1")
object AsyncServiceClient extends FutureClientOps

/**
 * So we need to take a type-parameterized request object, package it into a
 * monomorphic case class to send to the worker, and have the handler that
 * receives that object able to pattern match out the parameterized object, all
 * without using reflection.  We can do that with some nifty path-dependant
 * types
 */
class AsyncHandlerGenerator[C <: Protocol](config: ClientConfig, codec: Codec[C#Input,C#Output])(implicit sys: IOSystem) {

  type I = C#Input
  type O = C#Output

  case class PackagedRequest(request: I, response: Promise[O])

  class ClientWrapper(context: Context) extends WorkerItem(context) with ProxyActor {

    val client = new ServiceClient[C](codec, config, worker)

    def receive = {
      case PackagedRequest(request, promise) => {
        client.send(request).execute(promise.complete)
      }
      case FutureClient.GetConnectionStatus(promise) => {
        promise.success(client.connectionStatus)
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
