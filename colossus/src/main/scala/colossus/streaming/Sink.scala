package colossus.streaming

import scala.util.{Try, Success}

/**
 * A Sink is the write side of a pipe.  It allows you to push items to it,
 * and will return whether or not it can accept more data.  In the case where
 * the pipe is full, the Sink will return a mutable Trigger and you can
 * attach a callback to for when the pipe can receive more items
 */
trait Sink[-T] extends Transport {
  def push(item: T): PushResult

  def pushPeek: PushResult

  def canPush = pushPeek == PushResult.Ok

  def inputState: TransportState

  //after this is called, data can no longer be written, but can still be read until EOS
  def complete(): Try[Unit]

  def mapIn[A](f: A => T): Sink[A] = {
    val me = this
    new Sink[A] {
      def push(item: A) = me.push(f(item))
      def pushPeek = me.pushPeek
      def inputState = me.inputState
      def complete(): Try[Unit] = me.complete()
      def terminate(reason: Throwable): Unit = me.terminate(reason)
    }
  }

}

object Sink {

  /**
   * Create a sink from a function.  The Sink will always report itself as being
   * open and cannot be completed nor terminated
   */
  def open[T](f: T => PushResult) = new Sink[T] {
    def push(item: T) = f(item)

    def pushPeek = PushResult.Ok

    def inputState = TransportState.Open

    def complete() = Success(())

    def terminate(reason: Throwable){}
  }

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

    def pushPeek = PushResult.Ok
  }

  def valve[T](sink: Sink[T], valve: Sink[T]): Sink[T] = new Sink[T] {
    
    def push(item: T): PushResult = pushPeek match {
      case PushResult.Ok => {
        valve.push(item)
        sink.push(item)
      }
      case other => other
    }

    def pushPeek = valve.pushPeek match {
      case PushResult.Ok => sink.pushPeek
      case other => other
    }

    def inputState = sink.inputState

    def complete() = sink.complete()
    def terminate(reason: Throwable) {
      sink.terminate(reason)
      valve.terminate(reason)
    }
  }
}
