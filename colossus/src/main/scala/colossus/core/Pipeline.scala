package colossus.core

import scala.concurrent.duration._
import akka.actor.ActorRef

/**
 * This trait must be implemented by any non-head member of a pipeline.  
 */
trait HasUpstream[T] {
  private var _upstream: Option[T] = None
  def setUpstream(up: T) {
    _upstream = Some(up)
  }
  lazy val upstream = _upstream.getOrElse(throw new Exception("Attempt to use uninitialized upstream reference"))
}

/**
 * This must be implemented by any non-tail member of a pipeline
 */
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

/**
 * An `UpstreamEventHandler` is generally implemented by members of a pipline
 * that are neither the head nor tail.  This trait will ensure that events are
 * propagated to upstream neighbors
 */
trait UpstreamEventHandler[T <: UpstreamEvents] extends UpstreamEvents with HasUpstream[T]{

  override def shutdown() {
    super.shutdown()
    upstream.shutdown()
  }

}

/**
 * This is implemented by [[colossus.core.PipelineHandler]] and contains all the
 * methods made available to all layers extending the core layer
 */
trait ConnectionManager {
  /**
   * The current state of the underlying connection
   */
  def connectionState: ConnectionState

  /**
   * Gracefully shutdown the connection.  This will allow the connection handler
   * to go through its shutdown procedure.  Thus there is no guarantee of
   * exactly when the connection actually closes.
   */
  def disconnect()

  /**
   * Immediately shutdown the connection.  This skips any shutdown process.
   * This disconnection is not considered an error and the connection handler
   * will receive a [[DisconnectCause]] of `Disconnect`
   */
  def forceDisconnect()

  /**
   * Immediately shutdown the connection.  The disconnection will be treated as
   * an error and the ConnectionHandler will receive an `Error`
   * [[DisconnectCause]]
   */
  def kill(reason: Exception)

  /**
   * Replace the ConnectionHandler for this connection with a new one.  The
   * existing handler will go through its shutdown process before the switch is
   * made.  Returns false if the connection is not connected or is already in
   * the middle of another shutdown process, true otherwise
   */
  def become(nh: () => ConnectionHandler): Boolean

  def isConnected: Boolean

  /**
   * The context for the connection
   */
  def context: Context
}

/**
 * These are the methods the Core layer directly exposes to its downstream
 * neighbor which are generally not meant to be exposed further downstream
 */
trait CoreUpstream extends ConnectionManager  with UpstreamEvents {

  def requestWrite()

}
    

/**
 * These are the methods that the downstream neighbor of the CoreHandler must
 * implement
 */
trait CoreDownstream extends HasUpstream[CoreUpstream] with DownstreamEvents {

  def receivedData(data: DataBuffer)
  def readyForData(buffer: DataOutBuffer): MoreDataResult
}


/**
 * This trait must be implemented by the last stage of a pipeline
 */
trait HandlerTail extends UpstreamEvents


/**
 * The `PipelineHandler` forms the foundation of all pipeline-based connection
 * handlers.  It takes the head and tail of a pipeline and properly directs
 * events to it.
 */
class PipelineHandler(val downstream: CoreDownstream, val tail: HandlerTail) 
extends CoreHandler(downstream.context) with CoreUpstream with ServerConnectionHandler with ClientConnectionHandler with IdleCheck {

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

  def requestWrite() {
    connectionState match {
      case a: AliveState => a.endpoint.requestWrite()
      case _ => {} //maybe do something here?
    }
  }

}

object PipelineHandler {
  
  def apply(handler: CoreDownstream with HandlerTail): PipelineHandler = new PipelineHandler(handler, handler)
}
