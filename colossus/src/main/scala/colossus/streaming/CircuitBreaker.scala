package colossus.streaming

import scala.util.{Success, Try}

trait CircuitBreaker[T <: Transport] {

  protected var current: Option[T] = None

  protected var trigger = new Trigger()

  def set(item: T): Option[T] = {
    val t = current
    current = Some(item)
    while (current.isDefined && trigger.trigger()){}
    t
  }

  def unset(): Option[T] = {
    val t = current
    current = None
    t
  }

  def terminate(err: Throwable) {
    unset().foreach{_.terminate(err)}
  }

}

trait SourceCircuitBreaker[A, T <: Source[A]] extends  Source[A] { self: CircuitBreaker[T] => 

  def pull(): PullResult[A] = current match {
    case Some(c) => c.pull
    case None => PullResult.Empty(trigger)
  }

  def peek: PullResult[A] = current match {
    case Some(c) => c.peek
    case None => PullResult.Empty(trigger)
  }

  def outputState = TransportState.Open

  //these overrides are not required to compile, but do result in significant
  //performance improvements since it lets us invoke the optimized versions
  //inside BufferedPipe

  override def pullWhile(fn: NEPullResult[A] => PullAction) {
    current match {
      case Some(c) => c.pullWhile(fn)
      case None    => super.pullWhile(fn)
    }
  }

  override def pullUntilNull(fn: A => Boolean): Option[NullPullResult] = current match {
    case Some(c)  => c.pullUntilNull(fn)
    case None     => Some(PullResult.Empty(trigger))
  }

}

trait SinkCircuitBreaker[A, T <: Sink[A]] extends Sink[A] { self: CircuitBreaker[T] => 

  def inputState = TransportState.Open

  def push(item: A): PushResult = current match {
    case Some(c) => c.push(item)
    case None => PushResult.Full(trigger)
  }

  def pushPeek = current match {
    case Some(c) => c.pushPeek
    case None => PushResult.Full(trigger)
  }

  def complete(): Try[Unit] = {
    unset.map{_.complete()}.getOrElse(Success(()))
  }

}

class PipeCircuitBreaker[I, O] extends Pipe[I,O] with CircuitBreaker[Pipe[I,O]] with SourceCircuitBreaker[O, Pipe[I,O]] with SinkCircuitBreaker[I, Pipe[I,O]]

class Valve[I,O](pipe: Pipe[I,O]) extends PipeCircuitBreaker[I,O] {

  private var _isClosed = false
  set(pipe)

  def isClosed = _isClosed
  def isOpen = ! _isClosed

  def open() {
    _isClosed = false
    set(pipe)
  }

  def close() {
    _isClosed = true
    unset()
  }
}


