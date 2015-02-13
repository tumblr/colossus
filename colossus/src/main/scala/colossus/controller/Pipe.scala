package colossus
package controller

import core.DataBuffer
import akka.util.ByteString
import scala.util.{Try, Success, Failure}

import service.{Callback, UnmappedCallback}

/**
 * This trait is mostly intended for ByteBuffers which need to be copied to the
 * heap if they need to be buffered, basically in every other case copy can just return itself
 */
trait Copier[T] {
  def copy(item: T): T
}

object Copier {

  implicit object DataBufferCopier extends Copier[DataBuffer] {
    def copy(d: DataBuffer) = d.takeCopy
  }
}

//hrm....
object ReflexiveCopiers{

  trait Reflect[T] {
    def copy(t : T) : T = t
  }

  implicit object StringReflect extends Copier[String] with Reflect[String]
  implicit object IntReflect extends Copier[Int] with Reflect[Int]
  implicit object LongReflect extends Copier[Long] with Reflect[Long]
  implicit object ShortReflect extends Copier[Short] with Reflect[Short]
  implicit object FloatReflect extends Copier[Float] with Reflect[Float]
  implicit object DoubleReflect extends Copier[Double] with Reflect[Double]
}

trait Transport {
  //can be triggered by either a source or sink, this will immediately cause
  //all subsequent pull and pushes to return an Error
  def terminate(reason: Throwable)

  def terminated : Boolean
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

  //notice that Source has no equivalent of complete.  This is because we never
  //have a situation where a Source non-erroneously doesn't read the entire
  //stream
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


class Trigger {
  private var callback: Option[() => Unit] = None
  def fill(cb: () => Unit) {
    callback = Some(cb)
  }

  def trigger() {
    callback.foreach{f => f()}
  }

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

  //the item was successfully pushed but the pipe is not yet ready for more data, trigger is called when we're ready
  case class Filled(trigger: Trigger) extends Pushed

  //The pipe has been manually closed (without error) and is not accepting  any more items
  case object Closed extends NotPushed

  //The Pipe is currently full and will remain full until the previously returned trigger is activated
  case object Full extends NotPushed

  //The pipe has been terminated or some other error has occurred
  case class Error(reason: Throwable) extends NotPushed
}

/** A pipe designed to accept a fixed number of bytes
 * 
 * BE AWARE: when pushing buffers into this pipe, if the pipe completes, the
 * buffer may still contain unread data meant for another consumer
 */
class FiniteBytePipe(totalBytes: Long) extends InfinitePipe[DataBuffer, DataBuffer] {
  require(totalBytes >= 0, "A FiniteBytePipe must accept 0 or more bytes")

  private var taken = 0L
  def remaining = totalBytes - taken

  if(totalBytes == 0){
    complete()
  }

  def push(data: DataBuffer): PushResult = {
    //notice we're only dong these checks to avoid the databuffer copying,
    //won't need this when we get slices
    if (!terminated && !isFull) {
      if (taken == totalBytes) {
        throw new Exception("All bytes have been read but pipe is not closed!") //this should never happen
      } else {
        val partial = if (remaining >= data.remaining) {
          data
        } else {
          //TODO: need databuffer slices to avoid copying here
          DataBuffer(ByteString(data.take(math.min(remaining, Int.MaxValue).toInt)))
        }
        val res = internal.push(partial) 
        if (res == PushResult.Ok) {
          taken += partial.remaining
          if (taken == totalBytes) {
            complete()
          }
        }
        res
      }
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
}



sealed trait PipeException extends Throwable
class PipeTerminatedException(reason: Throwable) extends Exception("Pipe Terminated", reason) with PipeException

class InfinitePipe[T] extends Pipe[T, T] {

  sealed trait State
  case class Full(trigger: Trigger) extends State
  case class Pulling(callback: Try[Option[T]] => Unit) extends State
  case object Closed extends State
  case class Dead(reason: Throwable) extends State

  //Full is the default state because we can only push once we've received a callback from pull
  private var state: State = Full(new trigger)

  def terminated = state.isInstanceOf[Dead]
  def full = state.isInstanceOf[Full]

  /**
   * @return true if pipe can accept more data, false if this push filled the pipe
   * @throws PipeException when pushing to a full pipe
   */
  def push(item: T): PushResult = state match {
    case Full(trig)   => PushResult.Full(trig)
    case Dead(reason) => PushResult.Error(reason)
    case Closed       => PushResult.Closed
    case Pulling(cb)  => {
      state = Full(new Trigger)
      cb(Success(Some(item)))
      //notice that the callback may (and probably will) call "pull" in its execution.  This we can expect the state to have change after calling it
      state match {
        case
    }
  }
  
  //notice that whenReady is only called at most once.  This is done so the
  //consumer must explicitly call pull every time it needs more data, so can
  //properly apply backpressure
  // NOTE - whenReady's parameter has type Try[Option[T]] instead of some kind of ADT to be compatible with Callbacks
  def pull(whenReady: Try[Option[T]] => Unit): Unit = state match {
    case Dead(reason)  => whenReady(Failure(reason))
    case Closed        => whenReady(Success(None))
    case Pulling(_)    => whenReady(Failure(new Exception("Pipe already being pulled")))
    case Full(trig)    => {
      state = Pulling(whenReady)
      trig.trigger()
    }
  }
      
    
  def complete() {
    val oldstate = state
    state = Closed
    oldstate match {
      case Full(trig)   => trig.cancel()
      case Pulling(cb)  => cb(Success(None))
      case Dead(reason) => throw new PipeTerminatedException(reason) 
      case _ => {}
    }
  }

  def terminate(reason: Throwable) {
    val oldstate = state
    state = Dead(reason)
    oldstate match {
      case Full(trig)   => trig.cancel()
      case Pulling(cb)  => cb(Failure(new PipeTerminatedException(reason)))
      case Dead(r)      => throw new Exception("Cannot terminate a pipe that has already been terminated")
      case Closed       => {} //meh
    }
  }

}


