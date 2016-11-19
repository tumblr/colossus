package colossus.core

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
 * This is the connection handler on which the controller and service layers are
 * built.  It contains some common functionality that is ultimately exposed to
 * the users, such as methods to call for disconnecting and safely getting a
 * reference to the ConnectionHandle.  While there is no requirement to build
 * handlers on top of this one, it is recommended instead of directly
 * implementing the ConnectionHandler trait
 */
abstract class CoreHandler(ctx: Context) extends WorkerItem(ctx) with ConnectionHandler {
  import ConnectionState._

  private var shutdownAction: ShutdownAction = ShutdownAction.DefaultDisconnect
  private var _connectionState: ConnectionState = NotConnected

  def connectionState = _connectionState

  def isConnected = connectionState != NotConnected

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
        shutdown()
      }
      case NotConnected => shutdown()
      case _ => {}
    }
  }

  protected def shutdown() {
    shutdownAction match {
      case ShutdownAction.DefaultDisconnect | ShutdownAction.Disconnect => forceDisconnect()
      case ShutdownAction.Become(newHandlerFactory) => {
        worker.worker ! WorkerCommand.SwapHandler(newHandlerFactory())
      }
    }
  }


}

class BasicCoreHandler(context: Context) extends CoreHandler(context) with ServerConnectionHandler {

  protected def connectionClosed(cause: colossus.core.DisconnectCause): Unit = {}
  protected def connectionLost(cause: colossus.core.DisconnectError): Unit = {}
  def idleCheck(period: scala.concurrent.duration.Duration): Unit = {}
  def readyForData(buffer: DataOutBuffer): colossus.core.MoreDataResult = MoreDataResult.Complete
  def receivedData(data: colossus.core.DataBuffer): Unit = {}
  def receivedMessage(message: Any, sender: akka.actor.ActorRef){}

}
