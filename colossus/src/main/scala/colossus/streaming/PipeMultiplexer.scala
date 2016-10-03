package colossus
package streaming

import controller._

trait MultiStream[K, T] extends Stream[T] {
  def streamId(t: T): K
}

case class SubSource[K,T](id: K, stream: Source[T])
case class SubSink[K,T](id: K, stream: Sink[T])

object Multiplexing {
  
  /*

  def multiplex[K,T](base: Sink[T])(implicit ms: MultiStream[K,T]): Sink[SubSource[K,T]] = {
    var active = Map[K, Source[T]]()
    //sources add themselves to this when they are waiting to send data
    val sources = new BufferedPipe[Source[T]](10)
    var waitingToPush = false

    def feedBase() {
      if (!waitingToPush && !active.isEmpty) {
        var continue = true
        while (continue) {
          val it = active.toIterator
          if (!it.hasNext) {
            continue = false
          }
          while (it.hasNext) {
            val next = it.next
            next.pull() match {
              case PullResult.Item(i) => {
                base.push(i) match {
                  case PushResult.Full(
              }
              
      while (base.canPush) {
        val it = active.toIterator
        var stop = true
        while (it.hasNext && base.canPush && !stop) {
          it.next.pull() match {
            case PullResult.Item(i) => {
              stop = false
              base.push(i)
            }
            case PullResult.Empty(sig) => sig.notify{ feedBase() }
            //TODO : How should substream terminations be handled?
            case _ => {}
          }
        }
      }
    }

    sources.pullWhile{
      case PullRestult.Item(sub) => {
        active = active + (sub.id, sub.source)
        def doPull():Unit = sub.source.pullWhile{
          case PushResult.Item(item) => base.push(item) match {
            case PushResult.Full
        checkActive()
        true
      }
      case _ => false
    }


  }
  */

  def demultiplex[K,T](base: Source[T])(implicit ms: MultiStream[K,T]): Source[SubSource[K,T]] = {
    var active = Map[K, Pipe[T,T]]()
    def fatal(reason: String): Boolean = {
      base.terminate(new PipeStateException(reason))
      active.foreach{case (_, sink) => sink.terminate(new PipeStateException(reason))}
      false
    }
    val sources = new BufferedPipe[SubSource[K,T]](10)
    def doPull(): Unit = base.pullWhile {
      case PullResult.Item(item) => {
        println(s"GOT A $item")
        val id = ms.streamId(item)
        def tryPush(close: Boolean): Boolean = if (active contains id) {
          active(id).push(item) match {
            case PushResult.Full(signal) => {
              signal.notify{
                if (tryPush(close)) {
                  doPull()
                }
              }
              false
            }
            case PushResult.Closed => fatal(s"sub-pipe $id unexpectedly closed")
            case PushResult.Error(error) => fatal(s"sub-pipe error: $error")
            case other => {
              if (close) {
                active(id).complete()
                active = active - id
              }
              true
            }
          }
        } else {
          fatal(s"tried to push to non-existant stream $id")
        }
        ms.component(item) match {
          case StreamComponent.Head => if (active contains id) {
            fatal(s"duplicate id $id")
          } else {
            val n = new BufferedPipe[T](10)
            n.push(item)
            //TODO sources backpressure
            active = active + (id -> n)
            sources.push(SubSource(id, n))
            true
          }
          case StreamComponent.Body => tryPush(false)
          case StreamComponent.Tail => tryPush(true)
        }
      }
      case PullResult.Closed => fatal(s"Source Pipe closed unexpectedly")
      case PullResult.Error(error) => fatal(s"Source terminated with error: $error")
    }
    doPull()
    sources
  }
}
