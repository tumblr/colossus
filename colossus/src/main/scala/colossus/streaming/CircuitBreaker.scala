package colossus.streaming

import scala.util.Try

trait CircuitBreaker[T] {

  protected var current: Option[T] = None

  protected var trigger = new Trigger()

  def set(item: T): Option[T] = {
    val t = current
    current = Some(item)
    trigger.triggerAll()
    t
  }

  def unset(): Option[T] = {
    val t = current
    current = None
    t
  }

  def terminate(err: Throwable) {
    //??? wat do here
  }

}

trait SourceCircuitBreaker[A, T <: Source[A]] extends  Source[A] { self: CircuitBreaker[T] => 

  def pull(): PullResult[A] = current match {
    case Some(c) => c.pull
    case None => PullResult.Empty(trigger)
  }

  def peek: PullResult[Unit] = current match {
    case Some(c) => c.peek
    case None => PullResult.Empty(trigger)
  }

  def outputState = TransportState.Open

}

trait SinkCircuitBreaker[A, T <: Sink[A]] extends Sink[A] { self: CircuitBreaker[T] => 

  def inputState = TransportState.Open

  def push(item: A): PushResult = current match {
    case Some(c) => c.push(item)
    case None => PushResult.Full(trigger)
  }

  def complete(): Try[Unit] = {
    //uhhhh
    ???
  }

}

class PipeCircuitBreaker[I, O] extends CircuitBreaker[Pipe[I,O]] with SourceCircuitBreaker[O, Pipe[I,O]] with SinkCircuitBreaker[I, Pipe[I,O]]


