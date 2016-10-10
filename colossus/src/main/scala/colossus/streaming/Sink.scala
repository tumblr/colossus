package colossus.streaming

import scala.util.{Try, Success, Failure}

/**
 * A Sink is the write side of a pipe.  It allows you to push items to it,
 * and will return whether or not it can accept more data.  In the case where
 * the pipe is full, the Sink will return a mutable Trigger and you can
 * attach a callback to for when the pipe can receive more items
 */
trait Sink[T] extends Transport {
  def push(item: T): PushResult

  def inputState: TransportState

  //after this is called, data can no longer be written, but can still be read until EOS
  def complete(): Try[Unit]

}

object Sink {
  def blackHole[T]: Sink[T] = new Sink[T] {
    private var state: TransportState = TransportState.Open
    def push(item: T): PushResult = state match {
      case TransportState.Open => PushResult.Ok
      case TransportState.Closed => PushResult.Closed
      case TransportState.Terminated(err) => PushResult.Error(err)
    }
    def inputState = state
    def complete() = {
      state = TransportState.Closed
      Success(())
    }
    def terminate(reason: Throwable) {
      state = TransportState.Terminated(reason)
    }
  }
}
