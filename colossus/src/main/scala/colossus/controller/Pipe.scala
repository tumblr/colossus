package colossus
package controller

import core.{ConnectionHandler, DataBuffer, DataStream, WriteEndpoint, WriteStatus}
import akka.util.ByteString

import service.Codec

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

trait Transport {
  //can be triggered by either a source or sink, this will immediately cause
  //all subsquent pull and pushes to return an Error
  def terminate(reason: Throwable)
}

/**
 * A source is the write side of a pipe.  It allows you to push items to it,
 * and will return whether or not it can accept more data.  In the case where
 * the pipe is full, the source will return a mutable Trigger and you can
 * attach a callback to for when the pipe can receive more items
 */
trait Source[T] extends Transport {
  def push(item: T): PushResult

  //after this is called, data can no longer be written, but can still be read until EOS
  def complete()

}

/**
 * A Sink is the read side of a pipe.  You provide a handler for when an item
 * is ready and the sink will call it.  Note that if the underlying pipe has
 * multiple items ready, onReady will only be called once.  This is so that the
 * consumer of the sink can implicitly apply backpressure by only pulling when
 * it is able to
 */
trait Sink[T] extends Transport {
  def pull(onReady: PullResult[T] => Unit)

  //notice that Sink has no equivalent of complete.  This is because we never
  //have a situation where a sink non-erroneously doesn't read the entire
  //stream
}


/**
 * A Pipe is a callback-based data transport abstraction meant for handling
 * streams.  It provides backpressure feedback for both the write and read
 * ends.
 */
trait Pipe[T] extends Source[T] with Sink[T]

class Trigger {
  private var callback: Option[() => Unit] = None
  def fill(cb: () => Unit) {
    callback = Some(cb)
  }

  def trigger() {
    callback.foreach{f => f()}
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
  //an error occurred, due to pushing to a terminated, full, or complete source
  case class Error(reason: PipeException) extends PushResult
}

sealed trait PullResult[+T]
object PullResult {
  //pulled item
  case class Item[T](item: T) extends PullResult[T]
  //expect no more data
  case object End extends PullResult[Nothing]
  //an error occurred, possibly before all expected data was received
  case class Error(reson: PipeException) extends PullResult[Nothing]
}


//a pipe designed to accept a fixed number of bytes
class FiniteBytePipe(totalBytes: Int, maxSize: Int) extends Pipe[DataBuffer] {
  import PushResult._

  private val internal = new InfinitePipe[DataBuffer](maxSize)

  private var taken = 0

  def remaining = totalBytes - taken


  def push(data: DataBuffer): PushResult = {
    if (taken == totalBytes) {
      Done
    } else if (remaining >= data.remaining) {
      taken += data.size
      internal.push(data) 
    } else {
      val partial = DataBuffer(ByteString(data.take(remaining)))
      taken = totalBytes
      internal.push(partial)
      internal.complete()
      //we don't care if we fill the buffer cause we're done
      Done
    }
  }

  def pull(onReady: PullResult[DataBuffer] => Unit) {
    internal.pull(onReady)
  }

  //todo: possibly make better distinction between finite and infinite pipe
  def complete() {
    throw new Exception("Cannot complete a finite pipe")
  }

  def terminate(reason: Throwable) {
    internal.terminate(reason: Throwable)
  }
}



sealed trait PipeException extends Throwable
class PipeFullException(size: Int) extends Exception(s"Pipe is full (max size $size)") with PipeException
class PipeTerminatedException(reason: Throwable) extends Exception("Pipe Terminated", reason) with PipeException
class PipeClosedException extends Exception("Pipe Closed") with PipeException

class InfinitePipe[T : Copier](maxSize: Int) extends Pipe[T] {
  import PushResult.{Error => PushError, _}
  import PullResult.{Error => PullError, _}

  val queue = new java.util.LinkedList[T]

  //set by the puller when they pull and the queue is empty
  private var readyCallback: Option[PullResult[T] => Unit] = None

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
  def push(item: T): PushResult = if (terminated) {
    PushError(new PipeTerminatedException(termination.get))
  } else if (completed) {
    PushError(new PipeClosedException) 
  } else if (queue.size < maxSize) {
    readyCallback match{
      case Some(f) => {
        readyCallback = None
        f(Item(item))
        Ok
      }
      case None => {
        val copier = implicitly[Copier[T]]
        queue.add(copier.copy(item))
        if (queue.size == maxSize) {
          val t = new Trigger
          drainCallback = Some(t)
          Full(t)
        } else {
          Ok
        }
      }
    }
  } else {
    PushError(new PipeFullException(maxSize))
  }

  //notice that whenReady is only called at most once.  This is done so the
  //consumer must explicitly call pull every time it needs more data, so can
  //properly apply backpressure
  def pull(whenReady: PullResult[T] => Unit) {
    if (queue.size > 0) {
      whenReady(Item(queue.remove()))
      drainCallback.foreach { trigger => 
        drainCallback = None
        trigger.trigger()
      }
    } else {
      if (completed) {
        whenReady(End)
      } else if (terminated) {
        whenReady(PullError(new PipeTerminatedException(termination.get)))
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
      c(End)
    }
  }

  def terminate(reason: Throwable) {
    if (terminated) {
      throw new Exception("Cannot terminate a pipe that has already been terminated")
    }
    termination = Some(reason)
    readyCallback.foreach{ c => 
      readyCallback = None
      c(PullError(new PipeTerminatedException(reason)))
    }
    queue.clear()
  }

}



/** trait representing a decoded message that is actually a stream
 * 
 * When a codec decodes a message that contains a stream (perhaps an http
 * request with a very large body), the returned message MUST extend this
 * trait, otherwise the InputController will not know to push subsequent data
 * to the stream message and things will get all messed up. 
 */
trait StreamMessage {
  def source: Source[DataBuffer]
}

trait MessageHandler[Input, Output] {
  def codec: Codec[Output, Input]
}




