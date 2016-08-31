package colossus.core

import scala.concurrent.duration._
import akka.actor.ActorRef

sealed abstract class ShutdownAction(val rank: Int) {
  
  def >=(a: ShutdownAction): Boolean = rank >= a.rank

}

object ShutdownAction {
  case object DefaultDisconnect extends ShutdownAction(0)
  case class Become(newHandler: () => ConnectionHandler) extends ShutdownAction(1)
  case object Disconnect extends ShutdownAction(2)
}

sealed trait ConnectionState
sealed trait AliveState extends ConnectionState {
  def endpoint: WriteEndpoint
}

object ConnectionState {
  case object NotConnected extends ConnectionState
  case class Connected(endpoint: WriteEndpoint) extends ConnectionState with AliveState
  case class ShuttingDown(endpoint: WriteEndpoint) extends ConnectionState with AliveState
}
class InvalidConnectionStateException(state: ConnectionState) extends Exception(s"Invalid connection State: $state")


/**
 * These are methods made available to all layers extending the core layer
 */
trait ConnectionManager {
  def connectionState: ConnectionState
  def disconnect()
  def forceDisconnect()
  def become(nh: () => ConnectionHandler): Boolean

  def isConnected: Boolean
  def context: Context
}

/**
 * Methods for controlling whether the connection should be actively
 * reading/writing.  This trait is separate from Connection Manager and
 * CoreUpstream so that flow control can be selectively exposed to downstream layers
 */
trait FlowControl {
  def pauseReads()
  def pauseWrites()
  def resumeReads()
  def resumeWrites()
}



/**
 * These are the methods the Core layer directly exposes to its downstream
 * neighbor which are generally not meant to be exposed further downstream
 */
trait CoreUpstream extends ConnectionManager  with UpstreamEvents {

  //def requestWrite()


}

//upstream is set by the upstream itself
trait HasUpstream[T] {
  private var _upstream: Option[T] = None
  def setUpstream(up: T) {
    _upstream = Some(up)
  }
  def upstream = _upstream.getOrElse(throw new Exception("Attempt to use uninitialized upstream reference"))
}

//downstreams are provided through constructor parameters, so not much to add here
trait HasDownstream[T] {
  def downstream: T
}

/**
 * These are events that propagate to each layer starting from the head and moving downstream
 */
trait DownstreamEvents extends WorkerItemEvents {
  
  def connected() { onConnected() }
  def connectionTerminated(reason: DisconnectCause) { onConnectionTerminated(reason) }
  def idleCheck(period: FiniteDuration) { onIdleCheck(period) }
  def bind() { onBind() }
  def unbind() { onUnbind() }
  override def receivedMessage(message: Any, sender: ActorRef) { onReceivedMessage(sender, message) }

  protected def onConnected() {}
  protected def onConnectionTerminated(reason: DisconnectCause) {}
  protected def onIdleCheck(period: FiniteDuration){}
  protected def onReceivedMessage(sender: ActorRef, message: Any) {}

  //not really an event, but makes sense to go here for now, since currently the
  //tail (RequestHandler) is given the context
  def context: Context

}

/**
 * This trait can be used for layers that are in the head or middle of a
 * pipeline.  It will automatically propagate events to the downstream neighbor.
 * Notice that for each event, the onEvent method is called before propagating
 * the event downstream.
 */
trait DownstreamEventHandler[T <: DownstreamEvents] extends DownstreamEvents with HasDownstream[T] {
  override def connected() {
    super.connected()
    downstream.connected()
  }
  override def connectionTerminated(reason: DisconnectCause) {
    super.connectionTerminated(reason)
    downstream.connectionTerminated(reason)
  }
  override def idleCheck(period: FiniteDuration) {
    super.idleCheck(period)
    downstream.idleCheck(period)
  }
  override def bind() { 
    super.bind() 
    downstream.bind()
  }
  override def unbind() { 
    super.unbind()
    downstream.unbind()
  }
  override def receivedMessage(message: Any, sender: ActorRef) { 
    super.receivedMessage(message, sender)
    downstream.receivedMessage(message, sender)
  }

  def context = downstream.context


}

/**
 * These are events that propagate starting from the tail and move upstream
 */
trait UpstreamEvents {
  def shutdown() {
    onShutdown()
  }

  protected def onShutdown() {}





}

trait UpstreamEventHandler[T <: UpstreamEvents] extends UpstreamEvents with HasUpstream[T]{
  override def shutdown() {
    super.shutdown()
    upstream.shutdown()
  }


}
    

/**
 * These are the methods that the downstream neighbor of the CoreHandler must
 * implement
 */
trait CoreDownstream extends HasUpstream[CoreUpstream] with DownstreamEvents {

  def receivedData(data: DataBuffer)
  def readyForData(buffer: DataOutBuffer): MoreDataResult
}


trait HandlerTail extends UpstreamEvents {

}

/**
 * This is the connection handler on which the controller and service layers are
 * built.  It contains some common functionality that is ultimately exposed to
 * the users, such as methods to call for disconnecting and safely getting a
 * reference to the ConnectionHandle.  While there is no requirement to build
 * handlers on top of this one, it is recommended instead of directly
 * implementing the ConnectionHandler trait
 */
abstract class CoreHandler(val context: Context) extends ConnectionHandler {
  import ConnectionState._

  private var shutdownAction: ShutdownAction = ShutdownAction.DefaultDisconnect
  private var _connectionState: ConnectionState = NotConnected

  def connectionState = _connectionState
  def isConnected: Boolean = connectionState != ConnectionState.NotConnected


  private def setShutdownAction(action: ShutdownAction): Boolean = if (action >= shutdownAction) {
    shutdownAction = action
    true
  } else {
    false
  }

  def connected(endpt: WriteEndpoint) {
    connectionState match {
      case NotConnected => _connectionState = Connected(endpt)
      case other => throw new InvalidConnectionStateException(other)
    }
  }

  override def connectionTerminated(cause: DisconnectCause) {
    _connectionState = NotConnected
    super.connectionTerminated(cause)
  }

  /**
   * Returns a read-only trait containing live information about the connection.
   */
  final def connectionHandle: Option[ConnectionHandle] = connectionState match {
    case a: AliveState => Some(a.endpoint)
    case _ => None
  }

  /**
   * Close the underlying connection.  This is a "graceful" disconnect process,
   * in that any action mid-completion will be given a chance to finish what
   * they're doing before the connection actually closes.  For example, for a
   * service this will allow any requests being processed to complete.
   */
  final def disconnect() {
    setShutdownAction(ShutdownAction.Disconnect)
    shutdownRequest()
  }

  /**
   * Replace this connection handler with the given handler.  The actual swap
   * only occurs when the shutdown process complete
   */
  final def become(nh: () => ConnectionHandler): Boolean = if (setShutdownAction(ShutdownAction.Become(nh))) {
    shutdownRequest()
    true
  } else {
    false
  }

  /**
   * Immediately terminate the connection.  this is a kill action and completely
   * bypasses the shutdown process.
   */
  final def forceDisconnect() {
    connectionState match {
      case a: AliveState => a.endpoint.disconnect()
      case _ => {}
    }
  }

  final override def shutdownRequest() {
    connectionState match {
      case Connected(endpoint) => {
        _connectionState = ShuttingDown(endpoint)
        onShutdown()
      }
      case NotConnected => completeShutdown()
      case _ => {}
    }
  }

  def shutdown() {
    onShutdown()
  }

  protected def onShutdown() {
    completeShutdown()
  }

  final protected def completeShutdown() {
    shutdownAction match {
      case ShutdownAction.DefaultDisconnect | ShutdownAction.Disconnect => forceDisconnect()
      case ShutdownAction.Become(newHandlerFactory) => {
        worker.worker ! WorkerCommand.SwapHandler(newHandlerFactory())
      }
    }
  }



}

class PipelineHandler(val downstream: CoreDownstream, val tail: HandlerTail) 
extends CoreHandler(downstream.context) with CoreUpstream with ServerConnectionHandler with ClientConnectionHandler {

  downstream.setUpstream(this)

  override def onShutdown() {
    tail.shutdown()
  }

  override def shutdown() {
    completeShutdown()
  }

  override def connected(endpt: WriteEndpoint) {
    super.connected(endpt)
    downstream.connected()
  }

  def receivedData(buffer: DataBuffer){
    downstream.receivedData(buffer)
  }

  def readyForData(out: DataOutBuffer): MoreDataResult = downstream.readyForData(out)

  def idleCheck(period: FiniteDuration) {
    downstream.idleCheck(period)
  }

  override def onBind() {
    super.onBind()
    downstream.bind()
  }

  override def onUnbind() {
    super.onUnbind()
    downstream.unbind()
  }

  override def receivedMessage(message: Any, sender: ActorRef) {
    downstream.receivedMessage(message, sender)
  }
  protected def connectionClosed(cause: colossus.core.DisconnectCause): Unit = downstream.connectionTerminated(cause)
  protected def connectionLost(cause: colossus.core.DisconnectError): Unit = downstream.connectionTerminated(cause)

}

object PipelineHandler {
  
  def apply(handler: CoreDownstream with HandlerTail): PipelineHandler = new PipelineHandler(handler, handler)
}

/*
class BasicCoreHandler(context: Context) extends CoreHandler(context) with ServerConnectionHandler {

  protected def connectionClosed(cause: colossus.core.DisconnectCause): Unit = {}
  protected def connectionLost(cause: colossus.core.DisconnectError): Unit = {}
  def idleCheck(period: scala.concurrent.duration.Duration): Unit = {}
  def readyForData(buffer: DataOutBuffer): colossus.core.MoreDataResult = MoreDataResult.Complete
  def receivedData(data: colossus.core.DataBuffer): Unit = {}
  def receivedMessage(message: Any, sender: akka.actor.ActorRef){}

}
*/
