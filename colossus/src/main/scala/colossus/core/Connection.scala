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
  def id: Long
  def write(data: DataBuffer): WriteStatus
  def isWritable: Boolean
  def disconnect()
  def status: ConnectionStatus
  def sendMessage(message: Any)
  def worker: ActorRef
}

private[core] abstract class Connection(val id: Long, val key: SelectionKey, _channel: SocketChannel, val handler: ConnectionHandler)(implicit val sender: ActorRef) 
  extends LiveWriteBuffer with WriteEndpoint {

  import WorkerCommand._

  val startTime = System.currentTimeMillis

  /** ABSTRACT MEMBERS **/

  def domain: String
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
  def outgoing: Boolean //true for client, false for server
  def isTimedOut(time: Long): Boolean


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


  def sendMessage(message: Any) = {
    sender ! Message(id, message)
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

  def receivedMessage(message: Any, sender: ActorRef) = {
    handler.receivedMessage(message, sender)
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

  def isTimedOut(time: Long) = server.maxIdleTime != Duration.Inf && time - lastTimeDataReceived > server.maxIdleTime.toMillis

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

  def isTimedOut(time: Long) = false


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




