package colossus
package core

import akka.actor.ActorRef
import scala.concurrent.duration._


sealed trait MoreDataResult
object MoreDataResult {

  //the handler has no more data at the moment to write
  case object Complete extends MoreDataResult

  //the handler has more data to write as soon as it can
  case object Incomplete extends MoreDataResult
}

/**
 * This is the base trait for all connection handlers.  When attached to a
 * connection by a Delegator, these methods will be called in the worker's
 * thread.
 *
 * A ConnectionHandler is what directly interfaces with a Connection and is the reactor for all of the Connection events
 * that a Worker captures in its SelectLoop.
 */
trait ConnectionHandler extends WorkerItem {

  /**
   * Handler which is called when data is received from a Connection.
   * @param data  DataBuffer read from the underlying Connection.
   */
  def receivedData(data: DataBuffer)

  /**
   * Called from Worker when a connection has been terminated either by an error or by normal means.
   * @param cause why the connection was terminated
   */
  def connectionTerminated(cause : DisconnectCause) {
    cause match {
      case a : DisconnectError => connectionLost(a)
      case _ => connectionClosed(cause)
    }
  }

  /**
   * Connection was closed on our end, either by a shutdown, or by normal means
   * @param cause why the connection was closed
   */
  protected def connectionClosed(cause : DisconnectCause)

  /**
   * Connection lost is caused by termination, closed, etc
   * @param cause why the connection was lost
   */
  protected def connectionLost(cause : DisconnectError)


  /*
   * This event allows handlers to write data to the connection.  The output
   * buffer is limited in size so handlers must properly deal with backpressure.
   */
  def readyForData(buffer: DataOutBuffer) : MoreDataResult

  /**
   * This handler is called when a Worker new Connection is established.  A Connection can be
   * either an incoming (ie: something to connected to the server), or outgoing(ie: the server connected
   * to a remote system).
   * @param endpoint The endpoint which wraps the java NIO layer.
   */
  def connected(endpoint: WriteEndpoint)


}



/**
 * Mixin containing events just for server connection handlers
 */
trait ServerConnectionHandler extends ConnectionHandler {}


/**
 * ClientConnectionHandler is a trait meant to be used with outgoing connections.
 */
trait ClientConnectionHandler extends ConnectionHandler {

  /**
   * If no data is either sent or received in this amount of time, the connection is closed.  Defaults to Duration.Inf but handlers can override it
   */
  def maxIdleTime : Duration = Duration.Inf
}

/**
 * A Simple mixin trait that will cause the worker to not automatically unbind
 * this handler if the connection it's attached to is closed.  This mixin is
 * required if a connection handler wants to handle retry logic, since this
 * trait will allow it to continue to receive messages during the reconnection
 * process and bind to another connection
 */
trait ManualUnbindHandler extends ClientConnectionHandler


/**
 * A Watched handler allows an actor to be tied to a connection.  The worker
 * will watch the actor, and on termination will shutdown the connection
 * associated with the actor
 */
trait WatchedHandler extends ConnectionHandler {
  def watchedActor: ActorRef
}

/**
 * Convenience implementation of ConnectionHandler which provides implementations for all of
 * the necessary functions.  This allows for a devloper to extend this trait and only provide definitions
 * for the functions they require.
 */
abstract class BasicSyncHandler(context: Context) extends WorkerItem(context) with ConnectionHandler {

  def this(serverContext: ServerContext) = this(serverContext.context)

  private var _endpoint: Option[WriteEndpoint] = None
  def endpoint = _endpoint.getOrElse{
    throw new Exception("Handler is not connected")
  }
  def connected(e: WriteEndpoint) {
    _endpoint = Some(e)
  }
  def connectionClosed(cause : DisconnectCause){
    _endpoint = None
  }
  def connectionLost(cause : DisconnectError){
    _endpoint = None
  }
  def receivedMessage(message: Any, sender: ActorRef){}
  def readyForData(out: DataOutBuffer): MoreDataResult = MoreDataResult.Complete
  def idleCheck(period: Duration){}
  override def shutdownRequest (){
    endpoint.disconnect()
  }

  //this is the only method you have to implement
  //def receivedData(data: DataBuffer)
}

