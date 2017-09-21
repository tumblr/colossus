package colossus.streaming

import scala.util.{Try, Success, Failure}
import colossus.service.{Callback, UnmappedCallback}

sealed trait PullAction

/**
  * A PullAction is the return type of a processing function passed to
  * [[Source]].`pullWhile` method.  It signals to the source what action should
  * be taken after the processing function has processed the next item from the
  * source.
  */
object PullAction {

  /**
    * Pull the item just processed from the source and immediately process the next item when it becomes available
    */
  case object PullContinue extends PullAction

  /**
    * Pull the item just processed from the source and do not process any further items
    */
  case object PullStop extends PullAction //might not need this one

  /**
    * Do not pull the item from the source and stop processing
    */
  case object Stop extends PullAction //might not need this one

  /**
    * Do not pull the item from the source and halt processing until the provided signal is invoked
    */
  case class Wait(signal: Signal) extends PullAction

  /**
    * immediately terminate the source
    */
  case class Terminate(reason: Throwable) extends PullAction
}

/**
  * A `Source` is the read interface for a [[Pipe]].  Items can be pulled out of
  * the source if available.  When no item is available, a returned [[Signal]]
  * can be used to be notified when items are available.
  *
  * Sources can be mapped using the functionality provided in the
  * [[SourceMapper]] typeclass
  */
trait Source[+T] extends Transport {

  /**
    * Pull the next item from the Source if available.  The returned
    * [[PullResult]] will indicate whether an item was successfully pulled.
    */
  def pull(): PullResult[T]

  def peek: PullResult[T]

  def canPullNonEmpty = peek match {
    case PullResult.Item(_) => true
    case _                  => false
  }

  def outputState: TransportState

  def pull(whenReady: Try[Option[T]] => Unit): Unit = pull() match {
    case PullResult.Item(item)    => whenReady(Success(Some(item)))
    case PullResult.Error(err)    => whenReady(Failure(err))
    case PullResult.Closed        => whenReady(Success(None))
    case PullResult.Empty(signal) => signal.notify(pull(whenReady))
  }

  /**
    * Repeatedly pull items out of a pipe, even if items are not immediately
    * available.  The `Source` will hold onto the given processing function and
    * immediately forward items into it as they become available.  The returned
    * `PullAction` determines how the `Source` will proceed with the next item.
    * If `PullContinue` or `Wait` are returned, the `Source` will hold onto the
    * processing function for either when the next item is available or when the
    * returned `Signal` is fired.  `onComplete` is only called if the Source is
    * closed or terminated while the processing function is in use.
    *
    * When `Wait` is returned, the item that was passed into the processing
    * function is _not_ pulled from the source.  Thus when the returned signal is
    * fired and processing resumes, the same item will be passed to the
    * processing function.
    *
    * This method is generally intended for linking the output of a `Source` to
    * the input of a [[Sink]].  For a simplified version of this functionality, see
    * `Source.into`.
    */
  def pullWhile(fn: T => PullAction, onComplete: TerminalPullResult => Any)

  /**
    * Pull until either the supplied function returns false or there are no more
    * items immediately available to pull, in which case a `Some[NullPullResult]`
    * is returned indicating why the loop stopped.
    */
  def pullUntilNull(fn: T => Boolean): Option[NullPullResult] = {
    var done: Option[NullPullResult] = None
    while (pull() match {
             case PullResult.Item(item) => fn(item)
             case other => {
               done = Some(other.asInstanceOf[NullPullResult])
               false
             }
           }) {}
    done
  }

  def pullCB(): Callback[Option[T]] = UnmappedCallback(pull)

  def fold[U](init: U)(cb: (T, U) => U): Callback[U] = {
    pullCB().flatMap {
      case Some(i) => fold(cb(i, init))(cb)
      case None    => Callback.successful(init)
    }
  }

  def foldWhile[U](init: U)(cb: (T, U) => U)(f: U => Boolean): Callback[U] = {
    pullCB().flatMap {
      case Some(i) => {
        val aggr = cb(i, init)
        if (f(aggr)) {
          foldWhile(aggr)(cb)(f)
        } else {
          Callback.successful(aggr)
        }
      }
      case None => Callback.successful(init)
    }
  }

  def reduce[U >: T](reducer: (U, U) => U): Callback[U] = pullCB().flatMap {
    case Some(i) => fold[U](i)(reducer)
    case None    => Callback.failed(new PipeStateException("Empty reduce on pipe"))
  }

  def ++[U >: T](next: Source[U]): Source[U] = new DualSource(this, next)

  def collected: Callback[Iterator[T]] =
    fold(new collection.mutable.ArrayBuffer[T]) { case (next, buf) => buf append next; buf } map { _.toIterator }

  /**
    * Link this source to a sink.  Items will be pulled from the source and
    * pushed to the sink, respecting backpressure, until either the source is
    * closed or an error occurs.  The `linkClosed` and `linkTerminated`
    * parameters determine whether to propagate closure/termination of this
    * Source to the linked Sink.  However if the sink is closed or terminated first, this
    * source will be terminated.
    *
    * @param sink The sink to link to this source
    * @param linkClosed if true, the linked sink will be closed when this source is closed
    * @param linkTerminated if true, the linked sink will be terminated when this source is terminated
    */
  def into[X >: T](sink: Sink[X], linkClosed: Boolean, linkTerminated: Boolean)(
      onComplete: NonOpenTransportState => Any) {
    pullWhile(
      i =>
        sink.push(i) match {
          case PushResult.Ok            => PullAction.PullContinue
          case PushResult.Full(signal)  => PullAction.Wait(signal)
          case PushResult.Closed        => PullAction.Terminate(new PipeStateException("Downstream link unexpectedly closed"))
          case PushResult.Error(reason) => PullAction.Terminate(reason)
      }, {
        case PullResult.Closed => {
          if (linkClosed) {
            sink.complete()
          }
          onComplete(TransportState.Closed)
          PullAction.Stop
        }
        case PullResult.Error(err) => {
          if (linkTerminated) sink.terminate(err)
          onComplete(TransportState.Terminated(err))
          PullAction.Stop
        }
      }
    )
  }

  def into[X >: T](sink: Sink[X]) {
    into(sink, true, true)(_ => ())
  }

}

object Source {
  trait BasicMethods[T] extends Source[T] {
    def pullWhile(fn: T => PullAction, onComplete: TerminalPullResult => Any) {
      import PullAction._
      var continue = true
      while (continue) {
        continue = peek match {
          case PullResult.Empty(trig) => {
            trig.notify(pullWhile(fn, onComplete))
            false
          }
          case p @ PullResult.Item(i) =>
            fn(i) match {
              case PullContinue => {
                pull()
                true
              }
              case PullStop => {
                pull()
                false
              }
              case Stop => {
                false
              }
              case Wait(signal) => {
                signal.notify(pullWhile(fn, onComplete))
                false
              }
              case Terminate(reason) => {
                terminate(reason)
                onComplete(PullResult.Error(reason))
                false
              }
            }
          case PullResult.Closed => {
            onComplete(PullResult.Closed)
            false
          }
          case PullResult.Error(err) => {
            onComplete(PullResult.Error(err))
            false
          }
        }
      }
    }

  }

  class OneSource[T](data: T) extends Source[T] with BasicMethods[T] {
    var item: PullResult[T] = PullResult.Item(data)
    def pull(): PullResult[T] = {
      val t = item
      item match {
        case PullResult.Error(_) => {}
        case _                   => item = PullResult.Closed
      }
      t
    }

    def peek = item

    def terminate(reason: Throwable) {
      item = PullResult.Error(reason)
    }

    def outputState = item match {
      case PullResult.Error(err) => TransportState.Terminated(err)
      case PullResult.Closed     => TransportState.Closed
      case _                     => TransportState.Open
    }
  }

  /**
    * Create a source containing only one item
    */
  def one[T](data: T) = {
    new OneSource[T](data)
  }

  /**
    * Create a source containing items in the given array.  The source will be
    * closed after the last item in the array has been pulled
    */
  class ArraySource[T](arr: Array[T]) extends Source[T] {
    private var index = 0
    def hasNext       = index < arr.length
    def next = {
      index += 1
      arr(index - 1)
    }

    def pull(): PullResult[T] = if (index < arr.length) PullResult.Item(next) else PullResult.Closed

    def pullWhile(fn: T => PullAction, onComplete: TerminalPullResult => Any) {
      import PullAction._
      var continue = true
      while (continue) {
        if (hasNext) {
          fn(arr(index)) match {
            case PullContinue => {
              index += 1
            }
            case Wait(signal) => {
              signal.notify(pullWhile(fn, onComplete))
              continue = false
            }
            case _ => {
              continue = false
            }
          }
        } else {
          onComplete(PullResult.Closed)
          continue = false
        }
      }
    }

    def peek = if (hasNext) PullResult.Item(arr(index)) else PullResult.Closed

    def terminate(reason: Throwable) {}

    def outputState = if (hasNext) TransportState.Open else TransportState.Closed

  }
  def fromArray[T](arr: Array[T]): Source[T] = new ArraySource(arr)

  /**
    * Create a Source backed by the given iterator.  The source will closed after
    * the last item is pulled from the iterator.
    */
  def fromIterator[T](iterator: Iterator[T]): Source[T] = new Source[T] with BasicMethods[T] {
    //this will either be set to a Left (terminate was called) or a Right(complete was called)
    private var stop: Option[Throwable]   = None
    private var nextitem: NEPullResult[T] = if (iterator.hasNext) PullResult.Item(iterator.next) else PullResult.Closed

    def pull(): PullResult[T] = {
      nextitem match {
        case i @ PullResult.Item(_) => {
          val n = nextitem
          nextitem = if (iterator.hasNext) PullResult.Item(iterator.next) else PullResult.Closed
          n
        }
        case other => other
      }
    }

    def terminate(reason: Throwable) {
      nextitem = PullResult.Error(reason)
    }

    def peek = nextitem

    def outputState = nextitem match {
      case PullResult.Error(err) => TransportState.Terminated(err)
      case PullResult.Closed     => TransportState.Closed
      case _                     => TransportState.Open
    }

  }

  /**
    * Create a source that contains no items and is immediately closed
    */
  def empty[T] = new Source[T] with BasicMethods[T] {
    def pull()      = PullResult.Closed
    def peek        = PullResult.Closed
    def outputState = TransportState.Closed
    def terminate(reason: Throwable) {}
  }

  /**
    * Flatten a source of sources into a single source.  Sources pulled out of
    * the base source will be streamed into the flattened source one at a time.
    * Terminations are cascaded across all sources
    */
  def flatten[A](source: Source[Source[A]]): Source[A] = {
    val flattened = new BufferedPipe[A](1) //do we need to make this configurable?  probably not
    def killall(reason: Throwable) {
      flattened.terminate(reason)
      source.pullWhile(
        sub => {
          sub.terminate(reason)
          PullAction.PullContinue
        },
        _ => ()
      )
      source.terminate(reason)
    }
    def next(): Unit = source.pullWhile(
      subsource => {
        //this is a little weird, but eliminates recursion that could occur when
        //a bunch of sources are all pushed at once and all are able to complete
        //immediately.  We only want to do the recursive call of next() if the
        //sub-source does not immediately drain in this function call.  But if
        //the source is already closed by the very next line, then we know we're
        //ready for the next sub-source and can continue this pullWhile loop
        //println("pulled item")
        var callNext = false
        subsource.into(flattened, false, true) {
          case TransportState.Closed          => { if (callNext) next() }
          case TransportState.Terminated(err) => killall(err)
        }

        if (subsource.outputState != TransportState.Closed) {
          callNext = true
          PullAction.PullStop
        } else {
          PullAction.PullContinue
        }
      }, {
        case PullResult.Closed => {
          flattened.complete()
        }
        case PullResult.Error(err) => {
          killall(err)
        }
      }
    )
    next()
    flattened
  }
}

/**
  * Wraps 2 sinks and will automatically begin reading from the second only when
  * the first is empty.  The `None` from the first sink is never exposed.  The
  * first error reported from either sink is propagated.
  */
class DualSource[T](a: Source[T], b: Source[T]) extends Source[T] with Source.BasicMethods[T] {
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

  def peek = a.peek match {
    case PullResult.Closed => b.peek
    case other             => other
  }

  def terminate(reason: Throwable) {
    a.terminate(reason)
    b.terminate(reason)
  }

  def outputState = if (a_empty) b.outputState else a.outputState

  override def toString = s"DualSource($a,$b)"
}
