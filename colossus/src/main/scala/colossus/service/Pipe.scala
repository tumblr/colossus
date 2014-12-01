package colossus
package service

import core.{ConnectionHandler, DataBuffer, DataStream, WriteEndpoint, WriteStatus}
import akka.util.ByteString

/**
 * This trait is mostly intended for ByteBuffers which need to be copied to the
 * heap if they need to be buffered, basically in every other case copy can just return itself
 */
trait Copyable[T] {self: T => 
  def copy(): T
}

trait BasicCopyable[T] extends Copyable[T] { self: T =>
  def copy() = self
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

class InfinitePipe[T <: Copyable[T]](maxSize: Int) extends Pipe[T] {
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
        queue.add(item.copy())
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

/**
 * A mixin for ConnectionHandler that takes control of all read operations.
 * This will properly handle decoding messages, and for messages which are
 * streams, it will properly route data into the stream's source and handle
 * backpressure.
 */
trait InputController[Input, Output] extends ConnectionHandler with MessageHandler[Input, Output] {
  import PushResult._

  //todo: read-endpoint

  private var currentSource: Option[Source[DataBuffer]] = None
  private var currentTrigger: Option[Trigger] = None

  def receivedData(data: DataBuffer) {
    currentSource match {
      case Some(source) => source.push(data) match {
        case Ok => {}
        case Done => {
          currentSource = None
          //recurse since the databuffer may still contain data for the next request
          receivedData(data)
        }
        case Full(trigger) => {
          //TODO: disconnect reads and set trigger to re-enable reads
          currentTrigger = Some(trigger)
        }
        case Error(reason) => {
          //todo: what to do here?
        }
      }
      case None => codec.decode(data) match {
        case Some(message) =>  {
          message match {
            case s: StreamMessage => {
              currentSource = Some(s.source)
              //recurse to push rest of databuffer into source
              receivedData(data)
            }
            case _ => {}
          }
          processMessage(message)
        }
        case None => {}
      }
    }
  }


  protected def processMessage(message: Input)

}

//passed to the onWrite handler to indicate the status of the write for a
//message
sealed trait OutputResult
object OutputResult {

  // the message was successfully written
  case object Success   extends OutputResult

  // the message failed, most likely due to the connection closing partway
  case object Failure   extends OutputResult

  // the message was cancelled before it was written (not implemented yet) 
  case object Cancelled extends OutputResult
}

/**
 * A mixin for ConnectionHandler that takes control of all write operations.
 * This controller is designed to accept messages to write, where a message may
 * be a (possibly infinite) stream
 *
 */
trait OutputController[Input, Output] extends ConnectionHandler with MessageHandler[Input, Output]{

  import OutputResult._
  import PullResult._

  // == ABSTRACT MEMBERS ==
  
  // the write endpoint to control
  protected def writer: Option[WriteEndpoint]

  // maximum size of the pending write queue
  protected def maxQueueSize: Int

  // == PUBLIC / PROTECTED MEMBERS ==

  /// entry point for writing an item
  //todo: we should have another enum besides WriteStatus, since only Complete
  //or Failure will ever be exposed here
  def push(item: Output)(postWrite: OutputResult => Unit): Boolean = {
    if (waitingToSend.size < maxQueueSize) {
      waitingToSend.add(QueuedItem(item, postWrite))
      checkQueue() 
      true
    } else {
      false
    }
  }

  //messages being sent are failed, the rest are cancelled
  def purgeOutgoing() {
    currentlyWriting.foreach{queued => queued.postWrite(OutputResult.Failure)}
    currentlyWriting = None
    currentStream.foreach{case (queued, sink) => sink.terminate(new NotConnectedException("Connection closed"))}
    currentStream = None

  }

  def purgePending() {
    while (waitingToSend.size > 0) {
      val q = waitingToSend.remove()
      q.postWrite(Cancelled)
    }
  }

  def purgeAll() {
    purgeOutgoing()
    purgePending()
  }

  // == PRIVATE MEMBERS ==

  case class QueuedItem(item: Output, postWrite: OutputResult => Unit)

  private val waitingToSend = new java.util.LinkedList[QueuedItem]
  private var currentlyWriting: Option[QueuedItem] = None
  private var currentStream: Option[(QueuedItem, Sink[DataBuffer])] = None


  /*
   * iterate through the queue and write items.  Writing non-streaming items is
   * iterative whereas writing a stream enters drain, which will be recursive
   * if the stream has multiple databuffers to immediately write
   */
  private def checkQueue() {
    while (currentlyWriting.isEmpty && currentStream.isEmpty && waitingToSend.size > 0 && writer.isDefined) {
      val queued = waitingToSend.remove()
      codec.encode(queued.item) match {
        case DataStream(sink) => {
          drain(sink)
          currentStream = Some((queued, sink))
        }
        case d: DataBuffer => writer.get.write(d) match {
          case WriteStatus.Complete => {
            queued.postWrite(Success)
          }
          case _ => {
            currentlyWriting = Some(queued)
          }
        }
      }
    }
  }
      

  /*
   * keeps reading from a sink until it's empty or writing a databuffer is
   * incomplete.  Notice in the latter case we just wait for readyForData to be
   * called and resume there
   */
  private def drain(sink: Sink[DataBuffer]) {
    sink.pull{
      case Item(data) => writer.get.write(data) match {
        case WriteStatus.Complete => drain(sink)
        case _ => {} //todo: maybe do something on Failure?
      }
      case End => {
        currentStream.foreach{case (q, s) => 
          q.postWrite(OutputResult.Success)
        }
        currentStream = None
        checkQueue()
      }
      case Error(err) => {
        //TODO: what to do here?

      }
    }
  }

  /*
   * If we're currently streaming, resume the stream, otherwise if this is
   * called it means a non-stream item has finished fully writing, so we can go
   * back to checking the queue
   */
  def readyForData() {
    currentStream.map{case (q,s) => drain(s)}.getOrElse{
      currentlyWriting.foreach{q => q.postWrite(OutputResult.Success)}
      currentlyWriting = None
      checkQueue()
    }
  }

}


