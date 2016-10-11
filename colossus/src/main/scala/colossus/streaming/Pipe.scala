package colossus
package streaming

import core.DataBuffer
import akka.util.ByteString
import java.util.LinkedList
import scala.util.{Try, Success, Failure}

import service.{Callback, UnmappedCallback}
import scala.language.higherKinds



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
  require(size > 0, "buffer size must be greater than 0")

  sealed trait State
  sealed trait PushableState extends State
  //this is used when the consumer basically says "gimme all you got and I'll
  //tell you when to stop".  This is different from Pulling becuase in that case
  //there has to be a constant back-and-forth where each side signals that more
  //works can be done
  case class PullFastTrack(fn: NEPullResult[T] => Boolean) extends PushableState
  case object Closed extends State
  case object Active extends PushableState
  case class Dead(reason: Throwable) extends State

  private val pushTrigger = new Trigger
  private val pullTrigger = new Trigger

  private var state: State = Active
  private val buffer = new LinkedList[T]

  def inputState = state match {
    case p : PushableState => TransportState.Open
    case Closed => TransportState.Closed
    case Dead(reason) => TransportState.Terminated(reason)
  }
  def outputState = inputState

  def canPush: Boolean = state.isInstanceOf[PushableState]

  private def bufferFull = buffer.size >= size
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
    case Active if (bufferFull) => PushResult.Full(pushTrigger)
    case Active  => {
      //this state only occurs when somebody calls pull and the buffer is empty
      buffer.add(item)
      //if someone was waiting for items to enter the buffer, let them know now.
      //It's possible the notified consumer doesn't actually pull any data, so
      //keep notifying until the buffer is empty again
      while (!bufferEmpty && pullTrigger.trigger()) {}
      PushResult.Ok
    }
    case Dead(reason) => PushResult.Error(reason)
    case Closed       => PushResult.Closed
    case PullFastTrack(fn) => {
      //notice if we're in this state, then we know the buffer must be empty,
      //since it would have been drained into the fn
      if (!fn(PullResult.Item(item))) {
        state = Active
      }
      PushResult.Ok
    }
  }

  val hasItem = PullResult.Item[Unit](())

  def peek: PullResult[Unit] = state match {
    case Active => {  //either Active or Closed with data buffered
      if (bufferEmpty) {
        PullResult.Empty(pullTrigger)
      } else {
        hasItem
      }
    }
    case Dead(reason) => PullResult.Error(reason)
    case Closed if (buffer.size == 0) => PullResult.Closed
    case PullFastTrack(_) => PullResult.Error(new PipeStateException("cannot pull while fast-tracking"))
  }

  def pull(): PullResult[T] = state match {
    case Active => {  //either Active or Closed with data buffered
      if (bufferEmpty) {
        PullResult.Empty(pullTrigger)
      } else {
        val item = buffer.remove()
        while (!bufferFull && pushTrigger.trigger()) {}
        PullResult.Item(item)
      }
    }
    case Dead(reason) => PullResult.Error(reason)
    case Closed if (buffer.size == 0) => PullResult.Closed
    case PullFastTrack(_) => PullResult.Error(new PipeStateException("cannot pull while fast-tracking"))
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
  }


  def complete(): Try[Unit] = {
    val oldstate = state
    state = Closed
    pushTrigger.triggerAll() //alert anyone trying to push that the pipe is closed
    pullTrigger.triggerAll() //notice that there will only be listeners if the buffer is empty
    oldstate match {
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
    pushTrigger.triggerAll()
    pullTrigger.triggerAll()
    oldstate match {
      case PullFastTrack(fn)  => Success(fn(PullResult.Error(reason)))
      case _                  => {}
    }
  }

  override def pullWhile(fn: NEPullResult[T] => Boolean) {
    state match {
      case Dead(reason) => fn(PullResult.Error(reason))
      case Closed if (buffer.size == 0) => fn(PullResult.Closed)
      case _ => {
        val oldstate = state
        state = PullFastTrack(fn)
        var continue = true
        while (continue && buffer.size > 0) {
          continue = fn(PullResult.Item(buffer.remove()))
        }
        if (!continue) {
          state = oldstate
        }
      }
    }
  }

  override def pullUntilNull(fn: T => Boolean): Option[NullPullResult] = state match {
    case Dead(reason) => Some(PullResult.Error(reason))
    case Closed if (buffer.size == 0) => Some(PullResult.Closed)
    case _ => {
      var continue = true
      while (continue && buffer.size > 0) {
        val item = buffer.remove()
        continue = fn(item)
        println(s"$item - $continue - ${buffer.size}, $size")
      }
      if (continue) {
        Some(if (state == Closed) PullResult.Closed else PullResult.Empty(pullTrigger))
      } else {
        None
      }
    }

  }



}


  


