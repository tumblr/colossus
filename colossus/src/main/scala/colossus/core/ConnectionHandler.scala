package colossus
package core

import akka.actor.ActorRef
import scala.concurrent.duration._

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
   * Called from Worker when a connection has been terminated either by and error or by normal means.
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


  /**
   * This function is called to signal to the handler that it can resume writing data.
   * It is called as part of the WriteEndPoint event loop write cycle, where previously this handler
   * attempted to write data, but the buffers were filled up.  This is called once the buffers
   * are empty again and able to receive data.  This handler should be in a state where it is paused on writing
   * data until this handler is invoked.
   */
  def readyForData()

  /**
   * This handler is called when a Worker new Connection is established.  A Connection can be
   * either an incoming (ie: something to connected to the server), or outgoing(ie: the server connected
   * to a remote system).
   * @param endpoint The endpoint which wraps the java NIO layer.
   */
  def connected(endpoint: WriteEndpoint)

  /**
   * Called periodically on every attached connection handler, this can be used
   * for checking if an ongoing operation has timed out.
   *
   * Be aware that this is totally independant of a connection's idle timeout,
   * which is only based on the last time there was any I/O.
   *
   * @param period the frequency at which this method is called.  Currently this is hardcoded to `WorkerManager.IdleCheckFrequency`, but may become application dependent in the future.
   */
  def idleCheck(period: Duration)
}

/**
 * ClientConnectionHandler is a trait meant to be used with outgoing connections.  It provides a call back function
 * connectionFailed, which is called when a connection to an external system could not be established.
 */
trait ClientConnectionHandler extends ConnectionHandler {
  /**
   * Event handler for when a connection failed.
   */
  def connectionFailed()
}

/**
 * A Simple mixin trait that will cause the worker to automatically unbind this
 * handler if the connection it's attached to is closed.  This is generally a
 * good idea if the handler is not going to attempt any connection retry logic
 * or is not setup to receive any actor messages
 */
trait AutoUnbindHandler extends ClientConnectionHandler

/**
 * Convenience implementation of ConnectionHandler which provides implementations for all of
 * the necessary functions.  This allows for a devloper to extend this trait and only provide definitions
 * for the functions they require.
 */
trait BasicSyncHandler extends ConnectionHandler {
  private var _endpoint: Option[WriteEndpoint] = None
  def endpoint = _endpoint.getOrElse{
    throw new Exception("Handler is not connected")
  }
  private var _worker: Option[WorkerRef] = None
  def worker = _worker.getOrElse{
    throw new Exception("Handler is not bound to a worker")
  }
  def bound(id: Long, worker: WorkerRef) {
    _worker = Some(worker)
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
  def readyForData(){}
  def idleCheck(period: Duration){}

  //this is the only method you have to implement
  //def receivedData(data: DataBuffer)
}

