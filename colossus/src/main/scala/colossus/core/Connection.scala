package colossus
package core

import akka.actor.ActorRef

import scala.concurrent.duration._
import java.nio.channels.{SelectionKey, SocketChannel}

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
 * This is passed to handlers to give them a way to synchronously write to the
 * connection.  Services wrap this
 */
trait WriteEndpoint {

  /**
   * The id of the underlying connection.  This is different from a
   * WorkerItem's id.
   */
  def id: Long

  /**
   * Write some data to the connection.  Because connections are non-blocking,
   * it's possible that the underlying write buffer will fill up and not all of
   * the DataBuffer will be written.  When that occurs, the endpoint will keep
   * a copy of the unwritten data until the buffer empties.
   */

  def write(data: DataBuffer): WriteStatus

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
   * Get the current connection status.
   */
  def status: ConnectionStatus

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

  /**
   * Returns a timestamp, in milliseconds of when the last time data was written on the connection
   */
  def lastTimeDataWritten: Long
}

private[core] abstract class Connection(val id: Long, val key: SelectionKey, _channel: SocketChannel, val handler: ConnectionHandler)(implicit val sender: ActorRef)
  extends LiveWriteBuffer with WriteEndpoint {


  val startTime = System.currentTimeMillis

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

  /** ABSTRACT MEMBERS **/

  def domain: String
  def outgoing: Boolean //true for client, false for server
  def isTimedOut(time: Long): Boolean
  def unbindHandlerOnClose: Boolean


  protected val channel = _channel
  val worker = sender
  private var bytesReceived = 0L

  def info(now: Long): ConnectionInfo = {
    ConnectionInfo(
      domain = domain,
      host = _channel.socket.getInetAddress,
      port = port,
      id = id,
      timeOpen = now - startTime,
      timeIdle = now - lastTimeDataReceived,
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
    bytesReceived += data.size
    handler.receivedData(data)
  }


  def consoleString = {
    val now = System.currentTimeMillis
    val age = now - startTime
    val idle = now - _lastTimeDataReceived
    s"$id: $host:  age: $age idle: $idle"
  }

  def disconnect() {

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


  def onBufferClear() {
    handler.readyForData()
  }


}

private[core] class ServerConnection(id: Long, key: SelectionKey, channel: SocketChannel, handler: ConnectionHandler, val server: ServerRef)(implicit sender: ActorRef)
  extends Connection(id, key, channel, handler)(sender) {

  def domain: String = server.name.toString
  val outgoing: Boolean = false

  override def close(cause : DisconnectCause) = {
    server.server ! Server.ConnectionClosed(id, cause)
    super.close(cause)
  }

  def isTimedOut(time: Long) = server.maxIdleTime != Duration.Inf && time - math.max(lastTimeDataReceived, lastTimeDataWritten) > server.maxIdleTime.toMillis

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

  def isTimedOut(time: Long) = handler.maxIdleTime != Duration.Inf && time - math.max(lastTimeDataReceived, lastTimeDataWritten) > handler.maxIdleTime.toMillis

  def unbindHandlerOnClose = handler match {
    case u: ManualUnbindHandler => false
    case _ => true
  }


}

private[core] sealed trait RootDisconnectCause

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
  private[core] case object Unhandled extends RootDisconnectCause

  /**
   * We initiated the disconnection on our end
   */
  case object Disconnect extends DisconnectCause
  /**
   * The IO System is being shutdown
   */
  case object Terminated extends DisconnectCause

  /**
   * The connection was idle and timed out
   */
  case object TimedOut extends DisconnectError

  /**
   * Unknown Error was encountered
   */
  case class Error(error: Throwable) extends DisconnectError

  /**
   * The connection was closed by the remote end
   */
  case object Closed extends DisconnectError
}




