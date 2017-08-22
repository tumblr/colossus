package colossus.streaming

import java.util.LinkedList
import scala.util.{Try, Success, Failure}

/**
  * A Pipe is an abstraction that mediates interactions between producers and
  * consumers of a stream of data.  It can be thought of as a mutable buffer that
  * has built-in features for addressing both back-pressure (when the pipe
  * "fills") and forward-pressure (when the pipe "empties").  Items are "pushed"
  * into the pipe and "pulled" out of it.
  *
  * A Pipe is the combination of the [[Source]] and [[Sink]] traits, representing
  * the producer and consumer interfaces, respectively.  It should be noted that
  * a Pipe does not contain additional state beyond that provided by the Source
  * and Sink interfaces.  In other words, it may be possible for the producer
  * `Sink` side of a pipe to be Closed or Terminated, while the consumer `Source`
  * side is in a different state.
  *
  * The canonical implementation is the [[BufferedPipe]], backed by a
  * fixed-length buffer.  However pipes have many monadic and combinatorial
  * capabilities, allowing them to be mapped, linked together, flattened, and
  * multiplexed.
  *
  * As opposed to other libraries/frameworks that have a concept of streams,
  * sources, and sinks, these pipes are intended for low-level stream management.
  * They support several features that allow interation with pipes to be very
  * fast and efficient.  They form the backbone of all connection handlers and as
  * well as streaming protocols like http/2.  Pipes are *not* thread-safe.
  */
trait Pipe[I, O] extends Sink[I] with Source[O] {

  def weld[U >: O, T](next: Pipe[U, T]): Pipe[I, T] = {
    this into next
    new Channel(this, next)
  }

}

sealed trait PipeException                       extends Throwable
class PipeTerminatedException(reason: Throwable) extends Exception("Pipe Terminated", reason) with PipeException
class PipeStateException(message: String)        extends Exception(message) with PipeException

/**
  * A pipe backed by a fixed-length buffer.  Items can be pushed into the buffer
  * until it fills, at which point the `Full` [[PushResult]] is returned.  When
  * items are pulled out of the pipe the signal return in the Full result is
  * fired.
  *
  * The most efficient way to use a `BufferedPipe` is with the `pullWhile`
  * method.  This allows the pipe to completely bypass buffering and items pushed
  * to the pipe are fast-tracked directly into the provided processing function.
  */
class BufferedPipe[T](size: Int) extends Pipe[T, T] {
  require(size > 0, "buffer size must be greater than 0")

  sealed trait State
  sealed trait PushableState extends State
  //this is used when the consumer basically says "gimme all you got and I'll
  //tell you when to stop".  This is different from Pulling becuase in that case
  //there has to be a constant back-and-forth where each side signals that more
  //works can be done
  case class PullFastTrack(fn: T => PullAction, onComplete: TerminalPullResult => Any) extends PushableState
  case object Closed                                                                   extends State
  case object Active                                                                   extends PushableState
  case class Dead(reason: Throwable)                                                   extends State

  private val pushTrigger = new Trigger
  private val pullTrigger = new Trigger

  private var state: State = Active
  private val buffer       = new LinkedList[T]

  def inputState = state match {
    case p: PushableState => TransportState.Open
    case Closed           => TransportState.Closed
    case Dead(reason)     => TransportState.Terminated(reason)
  }
  def outputState = inputState

  def pushPeek: PushResult = state match {
    case p: PushableState =>
      if (bufferFull) {
        PushResult.Full(pushTrigger)
      } else {
        PushResult.Ok
      }
    case Closed       => PushResult.Closed
    case Dead(reason) => PushResult.Error(reason)
  }

  //used by ServiceClient, maybe there's a way to avoid this

  def length = buffer.size
  def head   = buffer.peek()

  private def bufferFull  = buffer.size >= size
  private def bufferEmpty = buffer.size == 0

  /** Attempt to push a value into the pipe.
    *
    * The value will only be successfully pushed only if there has already a
    * been a request for data on the pulling side.  In other words, the pipe
    * will never interally queue a value.
    *
    * @return the result of the push
    */
  def push(item: T): PushResult = state match {
    case PullFastTrack(fn, onc) =>
      fn(item) match {
        case PullAction.PullContinue => PushResult.Ok
        case PullAction.PullStop => {
          state = Active
          PushResult.Ok
        }
        case PullAction.Stop => {
          buffer.add(item)
          state = Active
          PushResult.Ok
        }
        case PullAction.Wait(signal) => {
          buffer.add(item)
          signal.notify(pullWhile(fn, onc))
          state = Active
          PushResult.Ok
        }
        case PullAction.Terminate(reason) => {
          terminate(reason)
          PushResult.Error(reason)
        }

      }
    case Active if (bufferFull) => PushResult.Full(pushTrigger)
    case Active => {
      buffer.add(item)
      //if someone was waiting for items to enter the buffer, let them know now.
      //It's possible the notified consumer doesn't actually pull any data, so
      //keep notifying until the buffer is empty again
      while (!bufferEmpty && pullTrigger.trigger()) {}
      PushResult.Ok
    }
    case Dead(reason) => PushResult.Error(reason)
    case Closed       => PushResult.Closed
  }

  def peek: PullResult[T] = state match {
    case Dead(reason)                 => PullResult.Error(reason)
    case Closed if (buffer.size == 0) => PullResult.Closed
    case PullFastTrack(_, _)          => PullResult.Error(new PipeStateException("cannot pull while fast-tracking"))
    case other => { //either Active or Closed with data buffered
      if (bufferEmpty) {
        PullResult.Empty(pullTrigger)
      } else {
        PullResult.Item(buffer.peek)
      }
    }
  }

  def pull(): PullResult[T] = state match {
    case Dead(reason)                 => PullResult.Error(reason)
    case Closed if (buffer.size == 0) => PullResult.Closed
    case PullFastTrack(_, _)          => PullResult.Error(new PipeStateException("cannot pull while fast-tracking"))
    case other => { //either Active or Closed with data buffered
      if (bufferEmpty) {
        PullResult.Empty(pullTrigger)
      } else {
        val item = buffer.remove()
        while (!bufferFull && pushTrigger.trigger()) {}
        PullResult.Item(item)
      }
    }
  }

  /**
    * Iterate through all buffered items, removing any where the provided
    * function returns true
    */
  def filterScan(f: T => Boolean) {
    var i = 0
    while (i < buffer.size) {
      if (f(buffer.get(i))) {
        buffer.remove(i)
      } else {
        i += 1
      }
    }
    while (!bufferFull && pushTrigger.trigger()) {}
  }

  def complete(): Try[Unit] = {
    val oldstate = state
    state = Closed
    pushTrigger.triggerAll() //alert anyone trying to push that the pipe is closed
    pullTrigger.triggerAll() //notice that there will only be listeners if the buffer is empty
    oldstate match {
      case PullFastTrack(_, onc) => Success(onc(PullResult.Closed))
      case Dead(reason)          => Failure(new PipeTerminatedException(reason))
      case _                     => Success(())
    }
  }

  //notice in all these cases, we are not storing a PipeTerminatedException,
  //but creating it on the spot every time so that the stack traces are more
  //accurate
  def terminate(reason: Throwable) {
    val oldstate = state
    state = Dead(new PipeTerminatedException(reason))
    pushTrigger.triggerAll()
    pullTrigger.triggerAll()
    oldstate match {
      case PullFastTrack(fn, onc) => Success(onc(PullResult.Error(reason)))
      case _                      => {}
    }
  }

  override def pullWhile(fn: T => PullAction, onComplete: TerminalPullResult => Any) {
    import PullAction._
    state match {
      case Dead(reason)                 => onComplete(PullResult.Error(reason))
      case Closed if (buffer.size == 0) => onComplete(PullResult.Closed)
      case _ => {
        val oldstate = state
        state = PullFastTrack(fn, onComplete)
        var continue = true
        while (continue && buffer.size > 0) {
          continue = fn(buffer.peek()) match {
            case PullContinue => {
              buffer.remove()
              true
            }
            case PullStop => {
              buffer.remove()
              false
            }
            case Stop         => false
            case Wait(signal) => { signal.notify(pullWhile(fn, onComplete)); false }
            case Terminate(reason) => {
              terminate(reason)
              false
            }
          }
        }
        if (!continue && state.isInstanceOf[PullFastTrack]) {
          state = oldstate
        }
        // notice if the buffer wasn't full when we started then there couldn't
        // have been any push triggers, so we don't need to check
        while (!bufferFull && pushTrigger.trigger()) {}
      }
    }
  }

  override def pullUntilNull(fn: T => Boolean): Option[NullPullResult] = state match {
    case Dead(reason)                 => Some(PullResult.Error(reason))
    case Closed if (buffer.size == 0) => Some(PullResult.Closed)
    case _ => {
      var continue = true
      val wasFull  = bufferFull
      while (continue && buffer.size > 0) {
        val item = buffer.remove()
        continue = fn(item)
      }
      if (continue) {
        if (wasFull) {
          //need to call pushTriggers
          state = PullFastTrack({ i =>
            fn(i) match {
              case true  => PullAction.PullContinue
              case false => PullAction.PullStop
            }
          }, _ => ())
          while (!bufferFull && pushTrigger.trigger()) {}
          //now at this point either the user fn returned false at some point or the pipe has been closed/terminated/emptied
        }
        state match {
          case Closed if (buffer.size == 0) => Some(PullResult.Closed)
          case Dead(reason)                 => Some(PullResult.Error(reason))
          case PullFastTrack(_, _) => {
            state = Active
            Some(PullResult.Empty(pullTrigger))
          }
          case _ => Some(PullResult.Empty(pullTrigger))
        }
      } else {
        None
      }
    }

  }

}
