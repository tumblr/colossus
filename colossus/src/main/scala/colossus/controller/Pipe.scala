package colossus
package controller

import core.DataBuffer
import akka.util.ByteString
import java.util.LinkedList
import scala.util.{Try, Success, Failure}

import service.{Callback, UnmappedCallback}
import scala.language.higherKinds

trait Transport {
  //can be triggered by either a source or sink, this will immediately cause
  //all subsequent pull and pushes to return an Error
  def terminate(reason: Throwable)

}

object Types {
  trait Functor[F[_]] {
    def map[A,B](a: F[A], f: A => B): F[B]
  }

  implicit class FunctorOps[F[_], A](val value: F[A]) extends AnyVal {
    def map[B](f: A => B)(implicit fct: Functor[F]): F[B] = fct.map(value, f)
  }
}
import Types._

sealed trait TransportState
object TransportState {
  case object Open extends TransportState
  case object Closed extends TransportState
  case class Terminated(reason: Throwable) extends TransportState
}

/**
 * A Sink is the write side of a pipe.  It allows you to push items to it,
 * and will return whether or not it can accept more data.  In the case where
 * the pipe is full, the Sink will return a mutable Trigger and you can
 * attach a callback to for when the pipe can receive more items
 */
trait Sink[T] extends Transport {
  def push(item: T): PushResult

  def inputState: TransportState

  //after this is called, data can no longer be written, but can still be read until EOS
  def complete(): Try[Unit]

  def feed(iterator: Iterator[T]) {
    var continue = true
    while (iterator.hasNext && continue) {
      this.push(iterator.next) match {
        case PushResult.Filled(sig) => {
          continue = false
          sig.react { _ => feed(iterator) }
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

object Sink {
  def blackHole[T]: Sink[T] = new Sink[T] {
    private var state: TransportState = TransportState.Open
    def push(item: T): PushResult = PushResult.Ok
    def inputState = state
    def complete() = {
      state = TransportState.Closed
      Success(())
    }
    def terminate(reason: Throwable) {
      state = TransportState.Terminated(reason)
    }
  }
}

sealed trait PullResult[+T]
sealed trait NEPullResult[+T] extends PullResult[T]
object PullResult {
  case class Item[T](item: T) extends NEPullResult[T]
  case class Empty[T](whenReady: Signal[NEPullResult[T]]) extends PullResult[T]
  case object Closed extends NEPullResult[Nothing]
  case class Error(reason: Throwable) extends NEPullResult[Nothing]

  implicit object NEPullResultMapper extends Functor[NEPullResult] {
    def map[A,B](p: NEPullResult[A], f: A => B): NEPullResult[B] = p match {
      case Item(i)  => Item(f(i))
      case Closed   => Closed
      case Error(r) => Error(r)
    }
  }
  implicit object PullResultMapper extends Functor[PullResult] {
    import Signal._
    implicit val s: Functor[Signal] = SignalMapper
    val x = implicitly[Functor[Signal]]
    def map[A,B](p: PullResult[A], f: A => B): PullResult[B] = p match {
      case Empty(sig) => Empty(sig.map{_.map(f)})
      case Item(i)    => Item(f(i))
      case Closed     => Closed
      case Error(r)   => Error(r)
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
  
  def pull(): PullResult[T]

  def outputState: TransportState

  def pull(whenReady: Try[Option[T]] => Unit): Unit = pull() match {
    case PullResult.Item(item)      => whenReady(Success(Some(item)))
    case PullResult.Error(err) => whenReady(Failure(err))
    case PullResult.Closed          => whenReady(Success(None))
    case PullResult.Empty(trig)     => trig.react{  
      case PullResult.Item(item)      => whenReady(Success(Some(item)))
      case PullResult.Error(err) => whenReady(Failure(err))
      case PullResult.Closed          => whenReady(Success(None))
    }
  }


  def pullWhile(fn: NEPullResult[T] => Boolean) {
    var continue = true
    while (continue) {
      continue = pull() match {
        case PullResult.Empty(trig) => {
          trig.react{x => 
            if (fn(x)) {
              pullWhile(fn)
            }
          }
          false
        }
        case p @ PullResult.Item(_) => fn(p) 
        case other => {
          //░░░░░░░░░░░░▄▐
          //░░░░░░▄▄▄░░▄██▄
          //░░░░░▐▀█▀▌░░░░▀█▄
          //░░░░░▐█▄█▌░░░░░░▀█▄
          //░░░░░░▀▄▀░░░▄▄▄▄▄▀▀
          //░░░░▄▄▄██▀▀▀▀
          //░░░█▀▄▄▄█░▀▀
          //░░░▌░▄▄▄▐▌▀▀▀
          //▄░▐░░░▄▄░█░▀▀ 
          //▀█▌░░░▄░▀█▀░▀
          //░░░░░░░▄▄▐▌▄▄
          //░░░░░░░▀███▀█░▄
          //░░░░░░▐▌▀▄▀▄▀▐▄
          //░░░░░░▐▀░░░░░░▐▌
          //░░░░░░█░░░░░░░░█
          //░░░░░▐▌░░░░░░░░░█
          //░░░░░█░░░░░░░░░░▐▌
          // you have been visited by the spooky skelton of ClassCastException!!! 
          //
          // Good performance will come to you
          //
          // but only if you .asInstanceOf 10 more types!!!
          fn(other.asInstanceOf[NEPullResult[T]])
          false
        }
      }
    }
  }

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

  def collected: Callback[Iterator[T]] = fold(new collection.mutable.ArrayBuffer[T]){ case (next, buf) => buf append next ; buf } map {_.toIterator}

  /**
   * Link this source to a sink.  Items will be pulled from the source and
   * pushed to the sink, respecting backpressure, until either the source is
   * closed or an error occurs.  The sink will be closed when this source is
   * closed.  If the sink is closed before this source, this source will be
   * terminated.  Other terminations are propagated in both directions.
   */
  def into[U >: T] (sink: Sink[U]) {
    def tryPush(item: T): Boolean = sink.push(item) match {
      case PushResult.Filled(sig) => {
        sig.react{_ => into(sink)}
        false
      }
      case PushResult.Full(sig) => {
        sig.react{_ => if (tryPush(item)) into(sink)}
        false
      }
      case PushResult.Closed => {
        terminate(new PipeStateException("downstream sink unexpectedly closed"))
        false
      }
      case PushResult.Error(err) => {
        terminate(err)
        false
      }
      case ok => true
    }
    def handlePull(r: NEPullResult[T]): Boolean =  r match {
      case PullResult.Item(item) => {
        tryPush(item)
      }
      case PullResult.Closed => {
        sink.complete()
        false
      }
      case PullResult.Error(err) => {
        sink.terminate(err)
        false
      }
    }
    pullWhile(handlePull)
  }

}

object PipeOps {

  implicit object SourceMapper extends Functor[Source]{
    def map[A,B](source: Source[A], fn: A => B): Source[B] = new Source[B] {
      def pull(): PullResult[B] = source.pull().map(fn)

      def outputState = source.outputState
      def terminate(err: Throwable) {
        source.terminate(err)
      }
      override def pullWhile(whilefn: NEPullResult[B] => Boolean) {
        source.pullWhile{x => whilefn(x.map(fn))}
      }
    }
  }

  //note - sadly trying to unify this with a HKT like Functor doesn't seem to
  //work since type inferrence fails on the type-lambda needed to squash
  //Pipe[_,_] down to M[_].  See: https://issues.scala-lang.org/browse/SI-6895
  
  implicit class PipeOps[A,B](val pipe: Pipe[A,B]) extends AnyVal {
    def map[C](fn: B => C): Pipe[A,C] = {
      val mappedsource = SourceMapper.map(pipe, fn)
      new Channel(pipe, mappedsource)
    }
  }

}

object Source {
  def one[T](data: T) = new Source[T] {
    var item: PullResult[T] = PullResult.Item(data)
    def pull(): PullResult[T] = {
      val t = item
      item match {
        case PullResult.Error(_) => {}
        case _ => item = PullResult.Closed
      }
      t
    }
    def terminate(reason: Throwable) {
      item = PullResult.Error(reason)
    }

    def outputState = item match {
      case PullResult.Error(err) => TransportState.Terminated(err)
      case PullResult.Closed => TransportState.Closed
      case _ => TransportState.Open
    }

  }

  def fromIterator[T](iterator: Iterator[T]): Source[T] = new Source[T] {
    //this will either be set to a Left (terminate was called) or a Right(complete was called)
    private var stop : Option[Throwable] = None
    def pull(): PullResult[T] = {
      stop match {
        case None => if (iterator.hasNext) {
          PullResult.Item(iterator.next)
        } else {
          PullResult.Closed
        }
        case Some(err) => PullResult.Error(err)
      }
    }

    def terminate(reason: Throwable) {
      stop = Some(reason)
    }

    def outputState = stop match {
      case Some(err) => TransportState.Terminated(err)
      case _ => if (iterator.hasNext) TransportState.Open else TransportState.Closed
    }


  }

  def empty[T] = new Source[T] {
    def pull() = PullResult.Closed 
    def outputState = TransportState.Closed
    def terminate(reason: Throwable){}
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
trait Pipe[I, O] extends Sink[I] with Source[O] {

  def weld[U >: O, T](next: Pipe[U,T]): Pipe[I, T] = {
    this into next
    new Channel(this, next)
  }

}

trait Signal[T] {
  def react(cb: T => Unit)
  def notify(cb: => Unit)
}

object Signal {
  
  implicit object SignalMapper extends Functor[Signal] {
    def map[A,B](sig: Signal[A], f: A => B): Signal[B] = new Trigger[B] {
      override def react(cb: B => Unit) {
        sig.react{a => cb(f(a))}
      }
      override def notify(cb: => Unit) {
        sig.notify(cb)
      }
    }
  }
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
class Trigger[T] extends Signal[T]{

  private var callbacks = new java.util.LinkedList[Either[T => Unit, () => Unit]]

  def empty = callbacks.size == 0

  def react(cb: T => Unit) {
    callbacks.add(Left(cb))
  }

  def notify(cb: => Unit) {
    callbacks.add(Right(() => cb))
  }

  def trigger(forNotify: => Unit, forReact: => T): Boolean = {
    if (callbacks.size == 0) false else {
      callbacks.remove() match {
        case Left(reactor) => reactor(forReact)
        case Right(notifier) => {forNotify; notifier()}
      }
      true
    }
  }

  def triggerAll(forNotify: => Unit, forReact: => T) {
    while (trigger(forNotify, forReact)) {}
  }

  def clear() {
    callbacks.clear()
  }

}

class BlankTrigger extends Trigger[Unit] {
  def trigger(): Boolean = {
    trigger(() , ())
  }

  def triggerAll() {
    while (trigger()) {}
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

  //the item was successfully pushed but the pipe is not yet ready for more data, trigger is called when it's ready
  case class Filled(onReady: Signal[Unit]) extends Pushed

  //the item was not pushed because the pipe is already full, the same trigger
  //returned when the Filled result was returned is included
  case class Full(onReady: Signal[Unit]) extends NotPushed

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
  def pull(): PullResult[T] = {
    if (a_empty) {
      b.pull()
    } else {
      val r = a.pull()
      r match {
        case PullResult.Closed => {
          a_empty = true
          b.pull()
        }
        case other => r
      }
    }
  }

  def terminate(reason: Throwable) {
    a.terminate(reason)
    b.terminate(reason)
  }

  def outputState = if (a_empty) b.outputState else a.outputState
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

class BufferedPipe[T](size: Int) extends Pipe[T, T] {

  sealed trait State
  sealed trait PushableState extends State
  case object Full extends State
  case object Pulling extends PushableState
  //this is used when the consumer basically says "gimme all you got and I'll
  //tell you when to stop".  This is different from Pulling becuase in that case
  //there has to be a constant back-and-forth where each side signals that more
  //works can be done
  case class PullFastTrack(fn: NEPullResult[T] => Boolean) extends PushableState
  case object Closed extends State
  case object Idle extends PushableState
  case class Dead(reason: Throwable) extends State

  private val pushTrigger = new BlankTrigger
  private val pullTrigger = new Trigger[NEPullResult[T]]

  //Full is the default state because we can only push once we've received a callback from pull
  private var state: State = Idle
  private val buffer = new LinkedList[T]

  def inputState = state match {
    case p : PushableState => TransportState.Open
    case Full => TransportState.Open
    case Closed => TransportState.Closed
    case Dead(reason) => TransportState.Terminated(reason)
  }
  def outputState = inputState

  def canPush: Boolean = state.isInstanceOf[PushableState]

  def attemptPush: PushResult = state match {
    case p: PushableState => PushResult.Ok
    case Full             => PushResult.Full(pushTrigger)
    case Closed           => PushResult.Closed
    case Dead(reason)     => PushResult.Error(reason)
  }

  /** Attempt to push a value into the pipe.
   *
   * The value will only be successfully pushed only if there has already a
   * been a request for data on the pulling side.  In other words, the pipe
   * will never interally queue a value.
   * 
   * @return the result of the push
   */
  def push(item: T): PushResult = state match {
    case Full         => PushResult.Full(pushTrigger)
    case Dead(reason) => PushResult.Error(reason)
    case Closed       => PushResult.Closed
    case PullFastTrack(fn) => {
      //notice if we're in this state, then we know the buffer must be empty,
      //since it would have been drained into the fn
      if (!fn(PullResult.Item(item))) {
        state = Idle
      }
      PushResult.Ok
    }
    case Pulling  => {
      //this state only occurs when somebody calls pull and the buffer is empty

      pullTrigger.trigger(
        forReact = PullResult.Item(item),
        forNotify = buffer.add(item)
      )
      //it's possible whoever we just notified didn't take the item, so try them
      //all if it was buffered
      while (buffer.size > 0 && pullTrigger.trigger(forReact = PullResult.Item(buffer.remove()), forNotify = ())) {}

      if (pullTrigger.empty && inputState == TransportState.Open) state = if (pushTrigger.empty || size > 0) Idle else Full
      PushResult.Ok
    }
    case Idle => {
      if (size == 0) {
        state = Full
        PushResult.Full(pushTrigger)
      } else {
        buffer.add(item)
        if (buffer.size >= size) {
          state = Full
          PushResult.Filled(pushTrigger)
        } else {
          PushResult.Ok
        }
      }
    }
  }

  def pull(): PullResult[T] = state match {
    case Dead(reason) => PullResult.Error(reason)
    case Closed if (buffer.size == 0) => PullResult.Closed
    case Pulling => PullResult.Empty(pullTrigger)
    case PullFastTrack(_) => PullResult.Error(new PipeStateException("cannot pull while fast-tracking"))
    case other => {  //either full, idle, or closed with data buffered
      if (buffer.size == 0) {
        val oldstate = state
        state = Pulling
        val maybeItem: Option[PullResult[T]] = oldstate match {
          case Full => {
            //in this state we know the buffer is empty, somebody tried to push,
            //AND nobody else is pulling.  To avoid buffering, we'll react on
            //the trigger ourselves so that the pushed item can be returned in
            //this function call
            var reacted: Option[PullResult[T]] = None
            pullTrigger.react{p => reacted = Some(p)}              
            pushTrigger.trigger()
            //if the producer we just notified did not try to push anything,
            //then our trigger is still sitting there and we need to clear it
            pullTrigger.clear()
            reacted
          }
          case _ => None
        }
        maybeItem match {
          case Some(i) => i
          case None => PullResult.Empty(pullTrigger)
        }
      } else {
        val item = buffer.remove()
        if (state == Full){
          state = Idle
          //now that we've freed up some space, we can start notifying producers
          //one at a time we fill up again
          while (canPush && pushTrigger.trigger()) {}
        }
        PullResult.Item(item)
      }
    }
  }


  def complete(): Try[Unit] = {
    val oldstate = state
    state = Closed
    oldstate match {
      case Full         => Success(pushTrigger.triggerAll())
      case Pulling  => Success(pullTrigger.triggerAll((), PullResult.Closed))
      case PullFastTrack(fn) => Success(fn(PullResult.Closed))
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
      case Full               => pushTrigger.triggerAll()
      case Pulling            => pullTrigger.triggerAll((), PullResult.Error(reason))
      case PullFastTrack(fn)  => Success(fn(PullResult.Error(reason)))
      case _                  => {}
    }
  }

  override def pullWhile(fn: NEPullResult[T] => Boolean) {
    state = PullFastTrack(fn)
    var continue = true
    while (continue && buffer.size > 0) {
      continue = fn(PullResult.Item(buffer.remove()))
    }
    if (!continue) {
      state = Idle
    }
  }



}

class Channel[I,O](sink: Sink[I], source: Source[O]) extends Pipe[I,O] {

  def push(item: I): PushResult = sink.push(item)

  def pull() = source.pull() 

  def outputState = source.outputState
  def inputState = sink.inputState

  def complete() = sink.complete()


  //TODO: This works fine when the termination is done on the channel, but what
  //happens if either the source or sink is independantly terminated?  Perhaps
  //we have to add a termination hook or something, or perhaps half-terminated channels don't matter?
  def terminate(reason: Throwable) {
    sink.terminate(reason)
    source.terminate(reason)
  }

}

object Channel {

  def apply[I,O]() : (Channel[I,O], Channel[O,I]) = {
    val inpipe = new BufferedPipe[I](10)
    val outpipe = new BufferedPipe[O](10)
    (new Channel(inpipe, outpipe), new Channel(outpipe, inpipe))
  }

}

  


