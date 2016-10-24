package colossus.streaming

import scala.util.{Success, Try}

trait CircuitBreaker[T <: Transport] {

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
    unset().foreach{_.terminate(err)}
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
    unset.map{_.complete()}.getOrElse(Success(()))
  }

}

class PipeCircuitBreaker[I, O] extends CircuitBreaker[Pipe[I,O]] with SourceCircuitBreaker[O, Pipe[I,O]] with SinkCircuitBreaker[I, Pipe[I,O]]


