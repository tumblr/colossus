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
  def push(item: T): Try[PushResult]

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
  //the item was successfully pushed
  case object Ok extends PushResult
  //for finite sources, the item was pushed and no more is expected
  case object Done extends PushResult
  //the item was pushed, but subsequent pushes will fail until trigger is fired
  case class Full(trigger: Trigger) extends PushResult
}

//a pipe designed to accept a fixed number of bytes
class FiniteBytePipe(totalBytes: Long, maxSize: Int) extends Pipe[DataBuffer, DataBuffer] {
  import PushResult._
  require(totalBytes >= 0, "A FiniteBytePipe must accept 0 or more bytes")

  private val internal = new InfinitePipe[DataBuffer](maxSize)

  private var taken = 0L

  def remaining = totalBytes - taken

  if(totalBytes == 0){
    internal.complete()
  }

  def push(data: DataBuffer): Try[PushResult] = {
    if (taken == totalBytes) {
      Success(Done)
    } else if (remaining > data.remaining) {
      taken += data.remaining
      internal.push(data) 
    } else {
      val partial = if (remaining == data.remaining) {
        data
      } else {
        DataBuffer(ByteString(data.take(math.min(remaining, Int.MaxValue).toInt)))
      }
      taken = totalBytes
      internal.push(partial)
      internal.complete()
      //we don't care if we fill the buffer cause we're done
      Success(Done)
    }
  }

  def pull(onReady: Try[Option[DataBuffer]] => Unit) {
    internal.pull(onReady)
  }

  //todo: possibly make better distinction between finite and infinite pipe
  def complete() {
    throw new Exception("Cannot complete a finite pipe")  //ASK:: WHY NOT??? What if its a finite stream that I only want an indeterminate subset of?(cough gifs, cough)
  }

  def terminate(reason: Throwable) {
    internal.terminate(reason: Throwable)
  }

  override def terminated: Boolean = internal.terminated
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
class PipeFullException(size: Int) extends Exception(s"Pipe is full (max size $size)") with PipeException
class PipeTerminatedException(reason: Throwable) extends Exception("Pipe Terminated", reason) with PipeException
class PipeClosedException extends Exception("Pipe Closed") with PipeException

//object EndOfStream extends Exception("End of Stream")

class InfinitePipe[T : Copier](maxSize: Int) extends Pipe[T, T] {
  import PushResult._

  val queue = new java.util.LinkedList[T]

  //set by the puller when they pull and the queue is empty
  private var readyCallback: Option[Try[Option[T]] => Unit] = None

  //set by the pusher when their last push fills the queue, called when it empties
  private var drainCallback: Option[Trigger] = None

  //set to true when there's no more data to push
  private var completed: Boolean = false

  //set to an error when terminate is called from either end
  private var termination: Option[Throwable] = None
  def terminated = termination.isDefined

  /**
   * @return true if pipe can accept more data, false if this push filled the pipe
   * @throws PipeException when pushing to a full pipe
   */
  def push(item: T): Try[PushResult] = if (terminated) {
    Failure(new PipeTerminatedException(termination.get))
  } else if (completed) {
    Failure(new PipeClosedException) 
  } else if (queue.size < maxSize) {
    readyCallback match{
      case Some(f) => {
        readyCallback = None
        f(Success(Some(item)))
        Success(Ok)
      }
      case None => {
        val copier = implicitly[Copier[T]]
        queue.add(copier.copy(item))
        if (queue.size == maxSize) {
          val t = new Trigger
          drainCallback = Some(t)
          Success(Full(t))
        } else {
          Success(Ok)
        }
      }
    }
  } else {
    Failure(new PipeFullException(maxSize))
  }

  //notice that whenReady is only called at most once.  This is done so the
  //consumer must explicitly call pull every time it needs more data, so can
  //properly apply backpressure
  def pull(whenReady: Try[Option[T]] => Unit) {
    if (queue.size > 0) {
      whenReady(Success(Some(queue.remove())))
      drainCallback.foreach { trigger => 
        drainCallback = None
        trigger.trigger()
      }
    } else {
      if (completed) {
        whenReady(Success(None))
      } else if (terminated) {
        whenReady(Failure(new PipeTerminatedException(termination.get)))
      } else {
        readyCallback = Some(whenReady)
      }
    }
  }


  def complete() {
    completed = true
    //notice if a callback  has been set, we know the queue is empty
    readyCallback.foreach{ c =>
      readyCallback = None
      c(Success(None))
    }
  }

  def terminate(reason: Throwable) {
    if (terminated) {
      throw new Exception("Cannot terminate a pipe that has already been terminated")
    }
    termination = Some(reason)
    readyCallback.foreach{ c => 
      readyCallback = None
      c(Failure(new PipeTerminatedException(reason)))
    }
    queue.clear()
  }

}


