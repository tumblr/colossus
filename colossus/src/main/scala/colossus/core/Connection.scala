package colossus
package core

import akka.actor.ActorRef

import scala.concurrent.duration._
import java.nio.channels.{SelectionKey, SocketChannel}
import java.net.InetSocketAddress

import encoding.DataOutBuffer

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
 * This is passed to handlers to give them a way to synchronously write to the
 * connection.  Services wrap this
 */
trait WriteEndpoint extends ConnectionInfo {

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
   * Terminate the connection
   */
  def disconnect()


  /**
   * Gets the worker this connection is bound to.
   *
   * todo: should be a WorkerRef.  Also is this even needed anymore?
   */
  def worker: ActorRef

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

private[core] abstract class Connection(val id: Long, val key: SelectionKey, _channel: SocketChannel, val handler: ConnectionHandler)(implicit val sender: ActorRef)
  extends LiveWriteBuffer with WriteEndpoint {


  val startTime = System.currentTimeMillis

  def remoteAddress = try {
    Some(_channel.getRemoteAddress.asInstanceOf[InetSocketAddress])
  } catch {
    case t: Throwable => None
  }

  //dont make these vals, doesn't work with client connections
  lazy val host: String = try {
    _channel.socket.getInetAddress.getHostName
  } catch {
    case n: NullPointerException => "[Disconnected]"
  }
  def port: Int = try {
    _channel.socket.getPort
  } catch {
    case n: NullPointerException => 0
  }

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

  protected val channel = _channel
  val worker = sender
  private var myBytesReceived = 0L

  def bytesReceived = myBytesReceived

  def info(now: Long): ConnectionSnapshot = {
    ConnectionSnapshot(
      domain = domain,
      host = _channel.socket.getInetAddress,
      port = port,
      id = id,
      timeOpen = now - startTime,
      readIdle = now - lastTimeDataReceived,
      writeIdle = now - lastTimeDataWritten,
      bytesSent = bytesSent,
      bytesReceived = bytesReceived
    )
  }

  def status = if (channel.isConnectionPending) {
    ConnectionStatus.Connecting
  } else if (channel.isConnected) {
    ConnectionStatus.Connected
  } else {
    ConnectionStatus.NotConnected
  }

  def isWritable = status == ConnectionStatus.Connected && !isDataBuffered

  private var _lastTimeDataReceived = startTime
  def lastTimeDataReceived = _lastTimeDataReceived

  def handleRead(data: DataBuffer)(implicit time: Long) = {
    _lastTimeDataReceived = time
    myBytesReceived += data.size
    handler.receivedData(data)
  }

  def requestWrite() {
    enableWriteReady()
  }

  def handleWrite(data: DataOutBuffer) {
    var more: MoreDataResult = MoreDataResult.Incomplete
    while (continueWrite() && more == MoreDataResult.Incomplete) {
      more  = handler.readyForData(data)
      write(data.data)
    }
    if (more == MoreDataResult.Complete) {
      disableWriteReady()
    }
  }


  def consoleString = {
    val now = System.currentTimeMillis
    val age = now - startTime
    val idle = timeIdle(now)
    s"$id: $host:  age: $age idle: $idle"
  }

  def disconnect() {
    //TODO: fix logic for graceful disconnect with partial buffer
    /*
    if (partialBuffer.isDefined) {
      disconnecting = true
    } else {
    */
      sender ! WorkerCommand.Disconnect(id)
    
  }

  /**
   * close is called with notify: false when reacting to the handler's termination
   * NOTE - This is only called by the worker, because otherwise the worker does not know about the connection being closed
   */
  def close(cause : DisconnectCause) {
    channel.close()
    handler.connectionTerminated(cause)
  }

}

private[core] class ServerConnection(id: Long, key: SelectionKey, channel: SocketChannel, handler: ServerConnectionHandler, val server: ServerRef)(implicit sender: ActorRef)
  extends Connection(id, key, channel, handler)(sender) {

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

private[core] class ClientConnection(id: Long, key: SelectionKey, channel: SocketChannel, handler: ClientConnectionHandler)(implicit sender: ActorRef)
  extends Connection(id, key, channel, handler)(sender) {

  def domain: String = "client" //TODO: fix this
  val outgoing: Boolean = true

  //used by workers to access the connectionFailed callback
  val clientHandler: ClientConnectionHandler = handler

  def handleConnected() = {
    channel.finishConnect()
    key.interestOps(SelectionKey.OP_READ)
    handler.connected(this)
  }

  def isTimedOut(time: Long) = isTimedOut(handler.maxIdleTime, time)

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




