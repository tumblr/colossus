package colossus.streaming

import scala.util.{Try, Success, Failure}
import colossus.service.{Callback, UnmappedCallback}

/**
 * A Source is the read side of a pipe.  You provide a handler for when an item
 * is ready and the Source will call it.  Note that if the underlying pipe has
 * multiple items ready, onReady will only be called once.  This is so that the
 * consumer of the sink can implicitly apply backpressure by only pulling when
 * it is able to
 */
trait Source[+T] extends Transport {
  
  def pull(): PullResult[T]

  def peek: PullResult[Unit]

  def canPullNonEmpty = peek match {
    case PullResult.Item(_) => true
    case _ => false
  }

  def outputState: TransportState


  def pull(whenReady: Try[Option[T]] => Unit): Unit = pull() match {
    case PullResult.Item(item)      => whenReady(Success(Some(item)))
    case PullResult.Error(err)      => whenReady(Failure(err))
    case PullResult.Closed          => whenReady(Success(None))
    case PullResult.Empty(signal)   => signal.notify(pull(whenReady))  
  }


  def pullWhile(fn: NEPullResult[T] => Boolean) {
    var continue = true
    while (continue) {
      continue = pull() match {
        case PullResult.Empty(trig) => {
          trig.notify(pullWhile(fn))
          false
        }
        case p @ PullResult.Item(_) => fn(p) 
        case PullResult.Closed => {
          fn(PullResult.Closed)
          false
        }
        case PullResult.Error(err) => {
          fn(PullResult.Error(err))
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
   *
   * @param sink The sink to link to this source
   * @param linkClosed if true, the linked sink will be closed when this source is closed
   * @param linkTerminated if true, the linked sink will be terminated when this source is terminated
   */
  def into[U >: T] (sink: Sink[U], linkClosed: Boolean, linkTerminated: Boolean)(onComplete: NonOpenTransportState => Any) {
    def tryPush(item: T): Boolean = sink.push(item) match {
      case PushResult.Full(signal) => {
        signal.notify{if (tryPush(item)) continue()}
        false
      }
      case PushResult.Closed => {
        val err = new PipeStateException("downstream sink unexpectedly closed")
        terminate(err)
        onComplete(TransportState.Terminated(err))
        false
      }
      case PushResult.Error(err) => {
        terminate(err)
        onComplete(TransportState.Terminated(err))
        false
      }
      case ok => true
    }
    def handlePull(r: NEPullResult[T]): Boolean =  r match {
      case PullResult.Item(item) => {
        tryPush(item)
      }
      case PullResult.Closed => {
        if (linkClosed) {
          sink.complete()
        }
        onComplete(TransportState.Closed)
        false
      }
      case PullResult.Error(err) => {
        if (linkTerminated) sink.terminate(err)
        onComplete(TransportState.Terminated(err))
        false
      }
    }
    def continue(): Unit = pullWhile(handlePull)
    continue
  }

  def into[U >: T] (sink: Sink[U]) {
    into(sink, true, true)( _ => ())
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

    def peek = item match {
      case PullResult.Item(_) => PullResult.Item(())
      case other => other.asInstanceOf[PullResult[Unit]]
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

  def fromArray[T](arr: Array[T]): Source[T] = fromIterator(new Iterator[T] {
    private var index = 0
    def hasNext = index < arr.length
    def next = {
      index += 1
      arr(index - 1)
    }
  })

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

    def peek = stop match {
      case None => if (iterator.hasNext) {
        PullResult.Item(())
      } else {
        PullResult.Closed
      }
      case Some(err) => PullResult.Error(err)
    }

    def outputState = stop match {
      case Some(err) => TransportState.Terminated(err)
      case _ => if (iterator.hasNext) TransportState.Open else TransportState.Closed
    }

    override def pullWhile(f: NEPullResult[T] => Boolean) {
      var continue = true
      while ( stop == None && iterator.hasNext && continue ){
        continue = f(PullResult.Item(iterator.next))
      }
      if (continue) {
        stop match {
          case Some(err) => f(PullResult.Error(err))
          case None =>  f(PullResult.Closed)
        }
      }
    }


  }

  def empty[T] = new Source[T] {
    def pull() = PullResult.Closed 
    def peek = PullResult.Closed
    def outputState = TransportState.Closed
    def terminate(reason: Throwable){}
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
      source.pullWhile{
        case PullResult.Item(sub) => {
          sub.terminate(reason)
          true
        }
        case _ => false
      }
      source.terminate(reason)
    }
    def next(): Unit = source.pullWhile{
      case PullResult.Item(subsource) => {
        //this is a little weird, but eliminates recursion that could occur when
        //a bunch of sources are all pushed at once and all are able to complete
        //immediately.  We only want to do the recursive call of next() if the
        //sub-source does not immediately drain in this function call.  But if
        //the source is already closed by the very next line, then we know we're
        //ready for the next sub-source and can continue this pullWhile loop
        //println("pulled item")
        var callNext = false
        var closed = false
        subsource.into(flattened, false, true) {
          case TransportState.Closed => {closed = true; if (callNext) next()}
          case TransportState.Terminated(err) => killall(err)
        }

        //we have to use this variable and not just check the state of the
        //subsource mainly due to the logic in Source.into that can result in a
        //"hanging item", where the source is closed but there is still one item
        //that is sitting in a callback function waiting to be pushed.  So we
        //instead need to rely on the callback passed to into, whch will be
        //triggered once that last item has been successfully pushed
        //TODO - this weirdness can be fixed if we can figure out a way to avoid these hanging items
        if (!closed) {
          callNext = true
        }
        closed
      }
      case PullResult.Closed => {
        flattened.complete()
        false
      }
      case PullResult.Error(err) => {
        killall(err)
        false
      }
    }
    next()
    flattened
  }
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

  def peek = if (a_empty) b.peek else a.peek

  def terminate(reason: Throwable) {
    a.terminate(reason)
    b.terminate(reason)
  }

  def outputState = if (a_empty) b.outputState else a.outputState
}
