package colossus.controller

import colossus.core.{DataOutBuffer, DisconnectCause, DisconnectError, MoreDataResult}
import colossus.streaming.{BufferedPipe, PipeCircuitBreaker, PipeStateException, PullResult}

abstract class StaticOutState(val canPush: Boolean) {
  def disconnecting = !canPush
}
object StaticOutState {
  case object Suspended     extends StaticOutState(true)
  case object Alive         extends StaticOutState(true)
  case object Disconnecting extends StaticOutState(false)
  case object Terminated    extends StaticOutState(false)
}

trait StaticOutputController[E <: Encoding] extends BaseController[E] {

  private def pipe = new BufferedPipe[E#Output](controllerConfig.outputBufferSize)

  val outgoing = new PipeCircuitBreaker[E#Output, E#Output]()

  private var state: StaticOutState = StaticOutState.Suspended
  private def disconnecting         = state.disconnecting
  private var _writesEnabled        = true

  def writesEnabled = _writesEnabled

  override def onConnected() {
    super.onConnected()
    outgoing.set(pipe)
    state = StaticOutState.Alive
    outgoing.peek match {
      case PullResult.Item(_)       => signalWrite()
      case PullResult.Empty(signal) => signal.notify { signalWrite() }
      case other                    => fatalError(new PipeStateException("Upstream in non-open state upon connection"), true)
    }
  }

  private def onClosed() {
    outgoing.unset()
  }

  protected def connectionClosed(cause: DisconnectCause) {
    state = StaticOutState.Terminated
    onClosed()
  }

  protected def connectionLost(cause: DisconnectError) {
    state = if (disconnecting) StaticOutState.Terminated else StaticOutState.Suspended
    onClosed()
  }

  private def signalWrite() {
    if (writesEnabled) {
      upstream.requestWrite()
    }
  }

  override def shutdown() {
    state = StaticOutState.Disconnecting
    checkShutdown()
  }

  private def checkShutdown() {
    if (disconnecting && !outgoing.canPullNonEmpty) {
      //we do this instead of shutting down immediately since this could be
      //called while in the middle of writing and end up prematurely closing the
      //connection
      signalWrite()
    }
  }

  def readyForData(buffer: DataOutBuffer) = {
    if (disconnecting && !outgoing.peek.isInstanceOf[PullResult.Item[_]]) {
      upstream.shutdown()
      MoreDataResult.Complete
    } else {
      val hasMore = outgoing.pullUntilNull { item =>
        codec.encode(item, buffer)
        !buffer.isOverflowed
      } match {
        case Some(PullResult.Empty(sig)) => {
          sig.notify { signalWrite() }
          false
        }
        case Some(PullResult.Error(uhoh)) => {
          fatalError(new PipeStateException(s"output stream unexpected terminated: $uhoh"), true)
          false
        }
        case Some(PullResult.Closed) => {
          fatalError(new PipeStateException("output stream unexpectedly closed"), true)
          false
        }
        case None => true //this would only occur if we returned false due to buffer overflowing
      }
      if (disconnecting || hasMore) {
        //return incomplete only if we overflowed the buffer and have more in
        //the queue, or if there's nothing left but we're disconnecting to
        //finish the disconnect process
        MoreDataResult.Incomplete
      } else {
        MoreDataResult.Complete
      }
    }
  }

}
