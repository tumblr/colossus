package colossus.streaming

import scala.util.{Success, Try}

trait CircuitBreaker[T <: Transport] {

  protected var current: Option[T] = None

  def isSet = current.isDefined

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

trait SourceCircuitBreaker[A, T <: Source[A]] extends  Source.BasicMethods[A] { self: CircuitBreaker[T] => 

  private def redirectFailure(p: PullResult[A]): PullResult[A] = {
    def emptyUnset = {
      unset()
      PullResult.Empty(trigger)
    }
    p match {
      case PullResult.Closed => emptyUnset
      case PullResult.Error(_) => emptyUnset
      case other => other
    }
  }

  def pull(): PullResult[A] = current match {
    case Some(c) => redirectFailure(c.pull)
    case None => PullResult.Empty(trigger)
  }

  def peek: PullResult[A] = current match {
    case Some(c) => redirectFailure(c.peek)
    case None => PullResult.Empty(trigger)
  }

  def outputState = TransportState.Open

  //these overrides are not required to compile, but do result in significant
  //performance improvements since it lets us invoke the optimized versions
  //inside BufferedPipe

  override def pullWhile(fn: A => PullAction, onc: TerminalPullResult => Any) {

    //so we're totally ignoring the passed-in terminal result handler, since
    //circuit breakers suppress closed/terminated
    
    def myonc(r: TerminalPullResult): Any = {
      unset()
      trigger.notify{pullWhile(fn, onc)}
    }

    current match {
      case Some(c) => c.pullWhile(fn, myonc)
      case None    => super.pullWhile(fn, myonc)
    }
  }

  override def pullUntilNull(fn: A => Boolean): Option[NullPullResult] = current match {
    case Some(c)  => c.pullUntilNull(fn) match {
      case Some(PullResult.Empty(t)) => Some(PullResult.Empty(t))
      case Some(_) => {
        unset()
        Some(PullResult.Empty(trigger))
      }
      case None => None
    }
    case None     => Some(PullResult.Empty(trigger))
  }

}

trait SinkCircuitBreaker[A, T <: Sink[A]] extends Sink[A] { self: CircuitBreaker[T] => 

  def inputState = TransportState.Open

  private def redirectFailure(res: PushResult) = {
    def fullUnset = {
      unset()
      PushResult.Full(trigger)
    }
    res match {
      case PushResult.Error(err)  => fullUnset
      case PushResult.Closed      => fullUnset
      case other                  => other
    }
  }

  def push(item: A): PushResult = current match {
    case Some(c) => redirectFailure(c.push(item))
    case None => PushResult.Full(trigger)
  }

  def pushPeek = current match {
    case Some(c) => redirectFailure(c.pushPeek)
    case None => PushResult.Full(trigger)
  }

  def complete(): Try[Unit] = {
    unset.map{_.complete()}.getOrElse(Success(()))
  }

}

class PipeCircuitBreaker[I, O] extends Pipe[I,O] with CircuitBreaker[Pipe[I,O]] with SourceCircuitBreaker[O, Pipe[I,O]] with SinkCircuitBreaker[I, Pipe[I,O]]

