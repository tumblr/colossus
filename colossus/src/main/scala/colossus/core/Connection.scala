package colossus
package core

import akka.actor.ActorRef

import scala.concurrent.duration._
import java.nio.channels.{SelectionKey, SocketChannel}
import java.net.InetSocketAddress

/**
 * Represent the connection state.  NotConnected, Connected or Connecting.
 */
sealed trait ConnectionStatus

object ConnectionStatus {
  case object NotConnected extends ConnectionStatus
  case object Connected extends ConnectionStatus
  case object Connecting extends ConnectionStatus
}

/**
 * A trait encapsulating live information about a connection.  This should only
 * contain read-only fields
 */
trait ConnectionInfo {

  /**
   * Get the current connection status.
   */
  def status: ConnectionStatus

  /**
   * The id of the underlying connection.  This is different from a
   * WorkerItem's id.
   */
  def id: Long

  /**
   * Returns a timestamp, in milliseconds of when the last time data was written on the connection
   */
  def lastTimeDataWritten: Long

  /**
   * Returns a timestamp, in milliseconds of when the last time data was read on the connection
   */
  def lastTimeDataReceived: Long

  /**
   * how many bytes have been sent on the connection in total
   */
  def bytesSent: Long

  /**
   * how many bytes have been received on the connection in total
   */
  def bytesReceived: Long

  /**
   * How long, in milliseconds, since the connection was opened
   */
  def timeOpen: Long

  /**
   * The address of the remote host for this connection, if connected
   */
  def remoteAddress: Option[InetSocketAddress]

}

/**
 * This trait contains all connection-level functions that should be accessable
 * to a top-level user.  It is primarily used by the Server DSL and subsequently
 * the Service DSL, where we want to give users control over
 * disconnecting/become, but not over lower-level tasks like requesting writes.
 *
 */
trait ConnectionHandle extends ConnectionInfo {

  /**
   * Terminate the connection
   */
  def disconnect()

  /**
   * Gets the worker this connection is bound to.
   */
  def worker: WorkerRef

}

/**
 * This is passed to handlers to give them a way to synchronously write to the
 * connection.  Services wrap this
 */
trait WriteEndpoint extends ConnectionHandle {

  /**
   * Signals to the worker that this connection wishes to write some data.  It
   * will trigger a call to the handler's readyForData when the connection is
   * able to write.
   */
  def requestWrite()

  /**
   * Returns true if data can be written to the connection.  This will
   * generally only return false if the write buffer is full or the connection
   * has been terminated.
   */
  def isWritable: Boolean

  /**
   * Disable all read events to the connection.  Once disabled, the connection
   * handler will stop receiving incoming data.  This can allow handlers to
   * provide backpressure to the remote host.
   */
  def disableReads()

  /**
   * Re-enable reads, if they were previously disabled
   */
  def enableReads()

}


abstract class Connection(val id: Long, initialHandler: ConnectionHandler, val worker: WorkerRef)
  extends WriteBuffer with WriteEndpoint {

  private var _handler: ConnectionHandler = initialHandler
  def handler = _handler

  /**
   * replace the existing handler with a new one.  The old handler is terminated
   * with the `Disconnect` cause and connected is called on the new handler if
   * the connection is connected.  No action is taken if the connection is closed
   */
  def setHandler(newHandler: ConnectionHandler) {
    if (status != ConnectionStatus.NotConnected) {
      handler.connectionTerminated(DisconnectCause.Disconnect)
      _handler = newHandler
      if (status == ConnectionStatus.Connected) {
        handler.connected(this)
      }
    }
  }

  val startTime = System.currentTimeMillis

  def remoteAddress: Option[InetSocketAddress] = None

  def timeOpen = System.currentTimeMillis - startTime


  /** ABSTRACT MEMBERS **/

  def domain: String
  def outgoing: Boolean //true for client, false for server
  def isTimedOut(time: Long): Boolean
  def unbindHandlerOnClose: Boolean

  def timeIdle(currentTime: Long) = currentTime - math.max(lastTimeDataReceived, lastTimeDataWritten)

  protected def isTimedOut(maxIdleTime: Duration, currentTime: Long): Boolean = {
    maxIdleTime != Duration.Inf && timeIdle(currentTime) > maxIdleTime.toMillis
  }

  private var myBytesReceived = 0L

  def bytesReceived = myBytesReceived

  def info(now: Long): ConnectionSnapshot = {
    ConnectionSnapshot(
      domain = domain,
      host = channelHost(),
      id = id,
      timeOpen = now - startTime,
      readIdle = now - lastTimeDataReceived,
      writeIdle = now - lastTimeDataWritten,
      bytesSent = bytesSent,
      bytesReceived = bytesReceived
    )
  }


  def isWritable = status == ConnectionStatus.Connected && !isDataBuffered

  private var _lastTimeDataReceived = startTime
  def lastTimeDataReceived = _lastTimeDataReceived

  def handleRead(data: DataBuffer)(implicit time: Long) = {
    _lastTimeDataReceived = time
    myBytesReceived += data.size
    handler.receivedData(data)
  }


  /**
   * The main entry point for allowing a connection handler to write data to the
   * connection.  Handlers cannot write data whenever they please, instead they
   * must initiate a write request.  The worker then calls this method to
   * fullfill the request and provide the handler with a DataOutBuffer to which
   * it can write data.
   */
  def handleWrite(data: DynamicOutBuffer): Boolean = {
    if (continueWrite()) {
      //the WriteBuffer is ready to accept more data, so let's get some.  Notice
      //this method (usually) only gets called if the handler had previously made a write
      //request, so it should have something to write
      val more  = handler.readyForData(data)
      val toWrite = data.data

      //its possible for the handler to not actually have written anything, this
      //can occur due to the fact that we use the writeReady flag to track both
      //pending data from the handler and from the partial buffer, so we can end up
      //calling handler.readyForData even when it didn't request a write
      val result = if (toWrite.remaining > 0) write(toWrite) else WriteStatus.Complete

      //we want to leave writeReady enabled if either the handler has more data
      //to write, or if the writebuffer couldn't write everything
      if (more == MoreDataResult.Complete && result == WriteStatus.Complete) {
        disableWriteReady()
      }
      true
    } else false
  }


  def disconnect() {
    super.gracefulDisconnect()
  }

  def completeDisconnect() {
    worker.worker ! WorkerCommand.Disconnect(id)
  }

  /**
   * close is called with notify: false when reacting to the handler's termination
   * NOTE - This is only called by the worker, because otherwise the worker does not know about the connection being closed
   */
  def close(cause : DisconnectCause) {
    channelClose()
    try {
      handler.connectionTerminated(cause)
    } catch {
      case t: Throwable => {
        //Notice that it's possible that an exception from somewhere else can
        //end up getting thrown here.  For example, if closing this connection
        //ends up terminating a pipe, the pipe's receive may throw an exception
        //here.  Since we're already closing the connection there's nothing else
        //to do here if this occurs
        //TODO: pending logging overhaul, this should log a warning
      }
    }
  }

}

/**
 * This mixin is used with all real connections, not in tests
 */
private[core] trait LiveConnection extends ChannelActions { self: Connection =>

  protected def channel: SocketChannel
  def key: SelectionKey

  def channelClose() { channel.close() }
  def finishConnect(){ channel.finishConnect() } //maybe move into a subtrait extending it

  def channelHost() = channel.socket.getInetAddress

  def channelWrite(raw: DataBuffer): Int = raw.writeTo(channel)

  def keyInterestOps(ops: Int) {
    key.interestOps(ops)
  }

  override def remoteAddress: Option[InetSocketAddress] = try {
    Some(channel.getRemoteAddress.asInstanceOf[InetSocketAddress])
  } catch {
    case t: Throwable => None
  }

  //dont make these vals, doesn't work with client connections
  lazy val host: String = try {
    channel.socket.getInetAddress.getHostName
  } catch {
    case n: NullPointerException => "[Disconnected]"
  }
  def port: Int = try {
    channel.socket.getPort
  } catch {
    case n: NullPointerException => 0
  }

  def status = if (channel.isConnectionPending) {
    ConnectionStatus.Connecting
  } else if (channel.isConnected) {
    ConnectionStatus.Connected
  } else {
    ConnectionStatus.NotConnected
  }


}

abstract class ServerConnection(
  id: Long,
  handler: ServerConnectionHandler,
  val server: ServerRef,
  worker: WorkerRef
) extends Connection(id, handler, worker) {

  def domain: String = server.name.toString
  val outgoing: Boolean = false

  def serverHandler: ServerConnectionHandler = handler

  override def close(cause : DisconnectCause) = {
    server.server ! Server.ConnectionClosed(id, cause)
    super.close(cause)
  }

  def isTimedOut(time: Long) = isTimedOut(server.maxIdleTime, time)

  def unbindHandlerOnClose = true

}

abstract class ClientConnection(
  id: Long,
  val clientHandler: ClientConnectionHandler,
  worker: WorkerRef
)  extends Connection(id, clientHandler, worker) {


  def domain: String = "client" //TODO: fix this
  val outgoing: Boolean = true

  def handleConnected() = {
    finishConnect()
    //TODO: is this needed?
    enableReads()
    handler.connected(this)
  }

  def isTimedOut(time: Long) = isTimedOut(clientHandler.maxIdleTime, time)

  def unbindHandlerOnClose = handler match {
    case u: ManualUnbindHandler => false
    case _ => true
  }


}

private[core] sealed trait RootDisconnectCause {
  def tagString: String
  def logString: String
}

/**
 * Messages representing why a disconnect occurred.  These can be either normal disconnects
 * or error cases
 */
sealed trait DisconnectCause extends RootDisconnectCause

/**
 * Subset of DisconnectCause which represent errors which resulted in a disconnect.
 */
sealed trait DisconnectError extends DisconnectCause

object DisconnectCause {

  //unhandled is private because it only occurs in situations where a
  //connection handler doesn't exist yet, so it doesn't make sense to force
  //handlers to handle a cause they will, by definition, never see
  private[core] case object Unhandled extends RootDisconnectCause {
    def tagString = "unhandled"
    def logString = "No Connection Handler Provided"
  }

  /**
   * We initiated the disconnection on our end
   */
  case object Disconnect extends DisconnectCause {
    def tagString = "disconnect"
    def logString = "Closed by local host"
  }
  /**
   * The IO System is being shutdown
   */
  case object Terminated extends DisconnectCause {
    def tagString = "terminated"
    def logString = "IO System is shutting down"
  }

  /**
   * The connection was idle and timed out
   */
  case object TimedOut extends DisconnectError {
    def tagString = "timedout"
    def logString = "Timed out"
  }

  /**
   * Unknown Error was encountered
   */
  case class Error(error: Throwable) extends DisconnectError {
    def tagString = "error"
    def logString = s"Error: $error"
  }

  /**
   * The connection was closed by the remote end
   */
  case object Closed extends DisconnectError {
    def tagString = "closed"
    def logString = "Closed by remote host"
  }

  /**
   * A client connection failed to connect
   */
  case class ConnectFailed(error: Throwable) extends DisconnectError {
    def tagString = "connectfailed"
    def logString = "Failed to Connect"
  }
}




