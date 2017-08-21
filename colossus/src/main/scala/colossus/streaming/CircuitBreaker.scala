package colossus.streaming

import scala.util.{Success, Try}

class InternalTransportClosedException extends Exception("Internal Transport unexpectedly Closed")

trait CircuitBreaker[T <: Transport] {

  protected var current: Option[T] = None

  def isSet = current.isDefined

  protected var trigger        = new Trigger()
  protected val onBreakTrigger = new Trigger()

  def onBreak(f: Throwable) {}

  def set(item: T): Option[T] = {
    val t = current
    current = Some(item)
    while (current.isDefined && trigger.trigger()) {}
    t
  }

  def unset(): Option[T] = {
    val t = current
    current = None
    t
  }

  def terminate(err: Throwable) {
    unset().foreach { _.terminate(err) }
  }

  protected def break(reason: Throwable) {
    unset()
    onBreak(reason)
  }

}

trait SourceCircuitBreaker[A, T <: Source[A]] extends Source.BasicMethods[A] { self: CircuitBreaker[T] =>

  private def redirectFailure(p: PullResult[A]): PullResult[A] = {
    def emptyUnset(reason: Throwable) = {
      break(reason)
      PullResult.Empty(trigger)
    }
    p match {
      case PullResult.Closed     => emptyUnset(new InternalTransportClosedException)
      case PullResult.Error(err) => emptyUnset(err)
      case other                 => other
    }
  }

  def pull(): PullResult[A] = current match {
    case Some(c) => redirectFailure(c.pull)
    case None    => PullResult.Empty(trigger)
  }

  def peek: PullResult[A] = current match {
    case Some(c) => redirectFailure(c.peek)
    case None    => PullResult.Empty(trigger)
  }

  def outputState = TransportState.Open

  //these overrides are not required to compile, but do result in significant
  //performance improvements since it lets us invoke the optimized versions
  //inside BufferedPipe

  override def pullWhile(fn: A => PullAction, onc: TerminalPullResult => Any) {

    //so we're totally ignoring the passed-in terminal result handler, since
    //circuit breakers suppress closed/terminated

    def myonc(r: TerminalPullResult): Any = {
      break(r match {
        case PullResult.Closed     => new InternalTransportClosedException
        case PullResult.Error(err) => err
      })
      trigger.notify { pullWhile(fn, onc) }
    }

    current match {
      case Some(c) => c.pullWhile(fn, myonc)
      case None    => super.pullWhile(fn, myonc)
    }
  }

  override def pullUntilNull(fn: A => Boolean): Option[NullPullResult] = current match {
    case Some(c) =>
      c.pullUntilNull(fn) match {
        case Some(PullResult.Empty(t)) => Some(PullResult.Empty(t))
        case Some(PullResult.Closed) => {
          break(new InternalTransportClosedException)
          Some(PullResult.Empty(trigger))
        }
        case Some(PullResult.Error(err)) => {
          break(err)
          Some(PullResult.Empty(trigger))
        }
        case None => None
      }
    case None => Some(PullResult.Empty(trigger))
  }

}

trait SinkCircuitBreaker[A, T <: Sink[A]] extends Sink[A] { self: CircuitBreaker[T] =>

  def inputState = TransportState.Open

  private def redirectFailure(res: PushResult) = {
    def fullUnset(reason: Throwable) = {
      break(reason)
      PushResult.Full(trigger)
    }
    res match {
      case PushResult.Error(err) => fullUnset(err)
      case PushResult.Closed     => fullUnset(new InternalTransportClosedException)
      case other                 => other
    }
  }

  def push(item: A): PushResult = current match {
    case Some(c) => redirectFailure(c.push(item))
    case None    => PushResult.Full(trigger)
  }

  def pushPeek = current match {
    case Some(c) => redirectFailure(c.pushPeek)
    case None    => PushResult.Full(trigger)
  }

  def complete(): Try[Unit] = {
    unset.map { _.complete() }.getOrElse(Success(()))
  }

}

class PipeCircuitBreaker[I, O](onBreakHandler: Throwable => Any = _ => ())
    extends Pipe[I, O]
    with CircuitBreaker[Pipe[I, O]]
    with SourceCircuitBreaker[O, Pipe[I, O]]
    with SinkCircuitBreaker[I, Pipe[I, O]] {

  override def onBreak(t: Throwable) {
    onBreakHandler(t)
  }
}
