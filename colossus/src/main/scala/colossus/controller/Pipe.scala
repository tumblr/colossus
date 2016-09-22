package colossus
package controller

import core.DataBuffer
import akka.util.ByteString
import java.util.LinkedList
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
  def complete(): Try[Unit]

  def feed(iterator: Iterator[T]) {
    var continue = true
    while (iterator.hasNext && continue) {
      this.push(iterator.next) match {
        case PushResult.Filled(sig) => {
          continue = false
          sig.react { feed(iterator) }
        }
        case other: PushResult.NotPushed => {
          //the pipe is closed or dead, so just stop feeding
          continue = false
        }
        case other => {}
      }
    }
    if (!iterator.hasNext) {
      this.complete()
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
trait Source[+T] extends Transport {
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

  def reduce[U >: T](reducer: (U, U) => U): Callback[U] = pullCB().flatMap {
    case Some(i) => fold[U](i)(reducer)
    case None => Callback.failed(new PipeStateException("Empty reduce on pipe"))
  }
    

  def ++[U >: T](next: Source[U]): Source[U] = new DualSource(this, next)

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

  def fromIterator[T](iterator: Iterator[T]): Source[T] = new Source[T] {
    //this will either be set to a Left (terminate was called) or a Right(complete was called)
    private var stop : Option[Throwable] = None
    def pull(on: Try[Option[T]] => Unit) {
      stop match {
        case None => if (iterator.hasNext) {
          on(Success(Some(iterator.next)))
        } else {
          on(Success(None))
        }
        case Some(err) => on(Failure(err))
      }
    }

    def terminate(reason: Throwable) {
      stop = Some(reason)
    }

    def terminated = stop match {
      case Some(_) => true
      case _ => false
    }

    def isClosed = !iterator.hasNext

  }
}


/**
 * A Pipe is a callback-based data transport abstraction meant for handling
 * streams.  It provides backpressure feedback for both the write and read
 * ends.
 *
 * Pipes are primarily a way to easily process incoming/outgoing streams and manage
 * backpressure.  A Producer pushes items into a pipe and a consumer pulls them
 * out.  Pulling is done through the use of a callback function which the Pipe
 * holds onto until an item is pushed.  Each call to `pull` will only ever pull one
 * item out of the pipe, so generally the consumer enters a loop by calling pull
 * within the callback function.
 * 
 * Backpressure is handled differently for the producer and consumer.  In effect,
 * the consumer is the "leader" in terms of backpressure, since the consumer must
 * always ask for more items.  For the producer, the return value of `push` will
 * indicate if backpressure is occurring.  When the pipe is "full", `push` returns
 * a `Trigger`, which the producer "fills" by supplying a callback function.  This
 * function will be called once the backpressure has been alleviated and the pipe
 * can accept more items.
 * 
 */
trait Pipe[T, U] extends Sink[T] with Source[U] {

}

trait Signal {
  def react(cb: => Unit)
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
class Trigger extends Signal{

  private var callback: Option[() => Unit] = None

  def react(cb: => Unit) {
    callback = Some(() => cb)
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

sealed trait PushResult {
  def pushed: Boolean
}
object PushResult {
  sealed trait Pushed extends PushResult {
    def pushed = true
  }
  sealed trait NotPushed extends PushResult {
    def pushed = false
  }

  //the item was successfully pushed and is ready for more data
  case object Ok extends Pushed

  //the item was pushed, but is approaching capacity, proactive backpressure measures should be taken if possible
  case object Filling extends Pushed

  //the item was successfully pushed but the pipe is not yet ready for more data, trigger is called when it's ready
  case class Filled(onReady: Signal) extends Pushed

  //the item was not pushed because the pipe is already full, the same trigger
  //returned when the Filled result was returned is included
  case class Full(onReady: Signal) extends NotPushed

  //The pipe has been manually closed (without error) and is not accepting any more items
  case object Closed extends NotPushed

  //The pipe has been terminated or some other error has occurred
  case class Error(reason: Throwable) extends NotPushed
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

  override def terminated: Boolean = if (a_empty) b.terminated else a.terminated

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

class BufferedPipe[T](size: Int, lowWatermarkP: Double = 0.8, highWatermarkP: Double = 0.9) extends Pipe[T, T] {

  sealed trait State
  sealed trait PushableState extends State
  case class Full(trigger: Trigger) extends State
  case class Pulling(callback: Try[Option[T]] => Unit) extends PushableState
  case object Closed extends State
  case object Idle extends PushableState
  case class Dead(reason: Throwable) extends State

  //Full is the default state because we can only push once we've received a callback from pull
  private var state: State = Idle
  private val buffer = new LinkedList[T]
  private val lowWatermark = size * lowWatermarkP
  private val highWatermark = size * highWatermarkP


  def terminated  = state.isInstanceOf[Dead]
  def isFull      = state.isInstanceOf[Full]
  def isClosed    = state == Closed

  def isPushable  = state.isInstanceOf[PushableState]

  /** Attempt to push a value into the pipe.
   *
   * The value will only be successfully pushed only if there has already a
   * been a request for data on the pulling side.  In other words, the pipe
   * will never interally queue a value.
   * 
   * @return the result of the push
   */
  def push(item: T): PushResult = state match {
    case Full(trig)   => PushResult.Full(trig)
    case Dead(reason) => PushResult.Error(reason)
    case Closed       => PushResult.Closed
    case Pulling(cb)  => {
      //notice we could only have been in this state if the consumer called pull
      //while the buffer was empty, so we know we can just directly send this
      //item to the puller
      state = Idle
      cb(Success(Some(item)))
      PushResult.Ok
    }
    case Idle => {
      if (size == 0) {
        val t = new Trigger
        state = Full(t)
        PushResult.Full(t)
      } else {
        buffer.add(item)
        if (buffer.size >= size) {
          val t = new Trigger
          state = Full(t)
          PushResult.Filled(t)
        } else if (buffer.size > highWatermark) {
          PushResult.Filling
        } else {
          PushResult.Ok
        }
      }
    }
  }

  /** Request the next value from the pipe
   * 
   * Only one value can be requested at a time.  Also there can only be one
   * outstanding request at a time.
   *
   * `whenReady`'s parameter has type Try[Option[T]] instead of some kind of ADT to be compatible with Callbacks
   */
  def pull(whenReady: Try[Option[T]] => Unit): Unit = state match {
    case Dead(reason)  => whenReady(Failure(reason))
    case Closed if (buffer.size == 0)  => whenReady(Success(None))
    case Pulling(_)    => whenReady(Failure(new PipeStateException("Pipe already being pulled")))
    case other => {
      if (buffer.size == 0) {
        val oldstate = state
        state = Pulling(whenReady)
        oldstate match {
          case Full(trig) => trig.trigger
          case _ => ()
        }            
      } else {
        whenReady(Success(Some(buffer.remove())))
        other match {
          case Full(trig) if (buffer.size <= lowWatermark) => {
            state = Idle
            trig.trigger()
          }
          case _ => ()
        }
      }
    }
  }
      
    
  def complete(): Try[Unit] = {
    val oldstate = state
    state = Closed
    oldstate match {
      case Full(trig)   => Success(trig.trigger())
      case Pulling(cb)  => Success(cb(Success(None)))
      case Dead(reason) => Failure(new PipeTerminatedException(reason))
      case _            => Success(())
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
      case _            => {}
    }
  }

}


