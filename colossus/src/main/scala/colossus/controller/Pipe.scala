package colossus
package controller

import core.DataBuffer
import akka.util.ByteString
import scala.util.{Try, Success, Failure}

import service.{Callback, UnmappedCallback}

trait Transport {
  //can be triggered by either a source or sink, this will immediately cause
  //all subsequent pull and pushes to return an Error
  def terminate(reason: Throwable)

  def terminated : Boolean

  def isClosed: Boolean
}

/**
 * A Sink is the write side of a pipe.  It allows you to push items to it,
 * and will return whether or not it can accept more data.  In the case where
 * the pipe is full, the Sink will return a mutable Trigger and you can
 * attach a callback to for when the pipe can receive more items
 */
trait Sink[T] extends Transport {
  def push(item: T): PushResult

  def isFull: Boolean

  //after this is called, data can no longer be written, but can still be read until EOS
  def complete()

  def feed(from: Source[T], linkState: Boolean) {
    def tryPush(item: T): Unit = push(item) match {
      case PushResult.Ok            => feed(from, linkState)
      case PushResult.Closed        => from.terminate(new Exception("This is probably not what we want to do"))
      case PushResult.Complete      => from.terminate(new Exception("This is probably not what we want to do"))
      case PushResult.Full(trig)    => trig.fill(() => tryPush(item))
      case PushResult.Filled(trig)  => trig.fill(() => tryPush(item))
      case PushResult.Error(reason) => from.terminate(reason)
    }
    from.pull{
      case Success(Some(item)) => tryPush(item)
      case Success(None)       => if (linkState) complete()
      case Failure(err)        => if (linkState) terminate(err)
    }
  }

}

/**
 * A Source is the read side of a pipe.  You provide a handler for when an item
 * is ready and the Source will call it.  Note that if the underlying pipe has
 * multiple items ready, onReady will only be called once.  This is so that the
 * consumer of the sink can implicitly apply backpressure by only pulling when
 * it is able to
 */
trait Source[T] extends Transport {
  def pull(onReady: Try[Option[T]] => Unit)

  def pullCB(): Callback[Option[T]] = UnmappedCallback(pull)

  def fold[U](init: U)(cb: (T, U) => U): Callback[U] = {
    pullCB().flatMap{
      case Some(i) => fold(cb(i, init))(cb)
      case None => Callback.successful(init)
    }
  }

  def foldWhile[U](init: U)(cb:  (T, U) => U)(f : U => Boolean) : Callback[U] = {
    pullCB().flatMap {
      case Some(i) => {
        val aggr = cb(i, init)
        if(f(aggr)){
              foldWhile(aggr)(cb)(f)
            }else{
              Callback.successful(aggr)
            }
        }
      case None => Callback.successful(init)
      }
  }


  def ++(next: Source[T]): Source[T] = new DualSource(this, next)

}

abstract class Generator[T] extends Source[T] {

  private var _terminated: Option[Throwable] = None
  private var _closed = false

  def terminated = _terminated.isDefined
  def isClosed = _closed

  def terminate(reason: Throwable) {
    _terminated = Some(reason)
  }

  def generate(): Option[T]

  def pull(f: Try[Option[T]] => Unit) {
    _terminated.map{t => f(Failure(new PipeTerminatedException(t)))}.getOrElse {
      val r = generate()
      if (r.isEmpty) _closed = true
      f(Success(r))
    }
  }
}

class IteratorGenerator[T](iterator: Iterator[T]) extends Generator[T] {
  def generate() = if (iterator.hasNext) Some(iterator.next) else None
}

object Source {
  def one[T](data: T) = new Source[T] {
    var item: Try[Option[T]] = Success(Some(data))
    def pull(onReady: Try[Option[T]] => Unit) {
      val t = item
      if (item.isSuccess) {
        item = Success(None)
      }
      onReady(t)
    }
    def terminate(reason: Throwable) {
      item = Failure(reason)
    }

    def terminated = item.isFailure
    def isClosed = item.filter{_.isEmpty}.isSuccess
  }
}


/**
 * A Pipe is a callback-based data transport abstraction meant for handling
 * streams.  It provides backpressure feedback for both the write and read
 * ends.
 */
trait Pipe[T, U] extends Sink[T] with Source[U] {

  def join[V, W](pipe : Pipe[V, W])(f : U => Seq[V]) : Pipe[T, W] = PipeCombinator.join(this, pipe)(f)

  def ->>[V, W](pipe : Pipe[V, W])(f : U => Seq[V]) : Pipe[T, W] = join(pipe)(f)
}


/**
 * When a user attempts to push a value into a pipe, and the pipe either fills
 * or was already full, a Trigger is returned in the PushResult.  This is
 * essentially just a fillable callback function that is called when the pipe
 * either becomes empty or is closed or terminated
 *
 * Notice that when the trigger is executed we don't include any information
 * about the state of the pipe.  The handler can just try pushing again to
 * determine if the pipe is dead or not.
 */
class Trigger {

  private var callback: Option[() => Unit] = None
  def fill(cb: () => Unit) {
    callback = Some(cb)
  }

  def trigger() {
    callback.foreach{f => f()}
  }

  /**
   * Cancels execution of the trigger.  The pusher should only do this when they're about to terminate the stream.
   *
   * TODO: might be a better way to handle this, but it would probably imply we'd need to know who terminated the stream
   */
  def cancel() {
    callback = None
  }

}

sealed trait PushResult
object PushResult {
  sealed trait Pushed extends PushResult
  sealed trait NotPushed extends PushResult

  //the item was successfully pushed and is ready for more data
  case object Ok extends Pushed

  //the item was successfully pushed but the pipe is not yet ready for more data, trigger is called when it's ready
  case class Filled(trigger: Trigger) extends Pushed

  //the item was successfully pushed but that's the last one, future pushes will return Closed
  case object Complete extends Pushed

  //the item was not pushed because the pipe is already full
  case class Full(trigger: Trigger) extends NotPushed

  //The pipe has been manually closed (without error) and is not accepting any more items
  case object Closed extends NotPushed

  //The pipe has been terminated or some other error has occurred
  case class Error(reason: Throwable) extends NotPushed
}

/** A pipe designed to accept a fixed number of bytes
 *
 * BE AWARE: when pushing buffers into this pipe, if the pipe completes, the
 * buffer may still contain unread data meant for another consumer
 */
class FiniteBytePipe(totalBytes: Long) extends InfinitePipe[DataBuffer] {
  require(totalBytes >= 0, "A FiniteBytePipe must accept 0 or more bytes")

  private var taken = 0L
  def remaining = totalBytes - taken

  if(totalBytes == 0){
    complete()
  }

  override def push(data: DataBuffer): PushResult = {
    //notice we're only dong these checks to avoid the databuffer copying,
    //won't need this when we get slices
    if (!terminated && !isFull && !isClosed) {
      if (taken == totalBytes) {
        throw new Exception("All bytes have been read but pipe is not closed!") //this should never happen
      } else {
        val partial = if (remaining >= data.remaining) {
          data
        } else {
          //TODO: need databuffer slices to avoid copying here
          DataBuffer(ByteString(data.take(remaining.toInt)))
        }
        //need to get this value here, since remaining might be 0 after call
        val toAdd = partial.remaining
        val res = super.push(partial)
        if (res.isInstanceOf[PushResult.Pushed]) {
          taken += toAdd
          if (taken == totalBytes) {
            complete()
            PushResult.Complete
          } else {
            res
          }
        } else {
          res
        }
      }
    } else {
      //lazy but effective, just push an empty buffer since it will be rejected anyway with the correct response
      super.push(DataBuffer(ByteString()))
    }
  }

}

/**
 * Wraps 2 sinks and will automatically begin reading from the second only when
 * the first is empty.  The `None` from the first sink is never exposed.  The
 * first error reported from either sink is propagated.
 */
class DualSource[T](a: Source[T], b: Source[T]) extends Source[T] {
  private var a_empty = false
  def pull(cb: Try[Option[T]] => Unit) {
    if (a_empty) {
      b.pull(cb)
    } else {
      a.pull{
        case Success(None) => {
          a_empty = true
          pull(cb)
        }
        case other => cb(other)
      }
    }
  }

  def terminate(reason: Throwable) {
    a.terminate(reason)
    b.terminate(reason)
  }

  override def terminated: Boolean = a.terminated && b.terminated

  def isClosed = a.isClosed && b.isClosed
}



sealed trait PipeException extends Throwable
class PipeTerminatedException(reason: Throwable) extends Exception("Pipe Terminated", reason) with PipeException
class PipeStateException(message: String) extends Exception(message) with PipeException

/**
 * This is a special exception that Input/Output controllers look for when
 * error handling pipes.  In most cases they will log the error that terminated
 * the pipe, but for this one exception, the failure will be silent.  This is
 * basically for situations where a certain amount of data is expected but for
 * some reason the receiver decides to cancel for some business-logic reason.
 */
class PipeCancelledException extends Exception("Pipe Cancelled") with PipeException

class InfinitePipe[T] extends Pipe[T, T] {

  sealed trait State
  case class Full(trigger: Trigger) extends State
  case class Pulling(callback: Try[Option[T]] => Unit) extends State
  case object Closed extends State
  case class Dead(reason: Throwable) extends State

  //Full is the default state because we can only push once we've received a callback from pull
  private var state: State = Full(new Trigger)

  def terminated  = state.isInstanceOf[Dead]
  def isFull      = state.isInstanceOf[Full]
  def isClosed    = state == Closed

  def isPushable  = state.isInstanceOf[Pulling]

  /** Attempt to push a value into the pipe.
   *
   * The value will only be successfully pushed only if there has already a
   * been a request for data on the pulling side.  In other words, the pipe
   * will never interally queue a value.
   *
   * @return the result of the push
   * @throws PipeException when pushing to a full pipe
   */
  def push(item: T): PushResult = state match {
    case Full(trig)   => PushResult.Full(trig)
    case Dead(reason) => PushResult.Error(reason)
    case Closed       => PushResult.Closed
    case Pulling(cb)  => {
      state = Full(new Trigger) //maybe create a new state here might be some weirdness if the puller pushes to the pipe
      cb(Success(Some(item)))
      //notice that the callback may (and probably will) call "pull" in its
      //execution.  Thus we can expect the state to have changed here
      state match {
        case Full(trig)   => PushResult.Filled(trig)    //cb didn't call "pull"
        case Dead(reason) => PushResult.Error(reason)   //cb terminated the pipe
        case Closed       => PushResult.Complete        //cb closed the pipe
        case Pulling(_)   => PushResult.Ok              //cb called pull
      }
    }
  }

  //this is useful for inherited pipes that need to do some processing on a value before it is pushed
  protected def whenPushable(f: => PushResult): PushResult = if (isPushable) f else state match {
    case Full(trig)   => PushResult.Full(trig)
    case Dead(reason) => PushResult.Error(reason)
    case Closed       => PushResult.Closed
    case Pulling(cb)  => throw new PipeStateException("This should never happen")
  }

  /** Request the next value from the pipe
   *
   * Only one value can be requested at a time.  Also there can only be one
   * outstanding request at a time.
   *
   */
  // NOTE - whenReady's parameter has type Try[Option[T]] instead of some kind of ADT to be compatible with Callbacks
  def pull(whenReady: Try[Option[T]] => Unit): Unit = state match {
    case Dead(reason)  => whenReady(Failure(reason))
    case Closed        => whenReady(Success(None))
    case Pulling(_)    => whenReady(Failure(new PipeStateException("Pipe already being pulled")))
    case Full(trig)    => {
      state = Pulling(whenReady)
      trig.trigger()
    }
  }


  def complete() {
    val oldstate = state
    state = Closed
    oldstate match {
      case Full(trig)   => trig.trigger()
      case Pulling(cb)  => cb(Success(None))
      case Dead(reason) => throw new PipeTerminatedException(reason)
      case _ => {}
    }
  }

  //notice in all these cases, we are not storing a PipeTerminatedException,
  //but creating it on the spot every time so that the stack traces are more
  //accurate
  def terminate(reason: Throwable) {
    val oldstate = state
    state = Dead(new PipeTerminatedException(reason))
    oldstate match {
      case Full(trig)   => trig.trigger()
      case Pulling(cb)  => cb(Failure(new PipeTerminatedException(reason)))
      case Dead(r)      => throw new PipeStateException("Cannot terminate a pipe that has already been terminated")
      case Closed       => {} //don't think there's anything to do here
    }
  }

}


