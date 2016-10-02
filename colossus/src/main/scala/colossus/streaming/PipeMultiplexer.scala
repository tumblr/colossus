package colossus
package streaming

import controller._

trait MultiStream[K, T] extends Stream[T] {
  def streamId(t: T): K
}

case class Substream[K,T](id: K, stream: Source[T])

object Multiplexing {

  def demultiplex[K,T](base: Source[T])(implicit ms: MultiStream[K,T]): Source[Substream[K,T]] = {
    var active = Map[K, Pipe[T,T]]()
    def fatal(reason: String): Boolean = {
      base.terminate(new PipeStateException(reason))
      active.foreach{case (_, sink) => sink.terminate(new PipeStateException(reason))}
      false
    }
    val sources = new BufferedPipe[Substream[K,T]](10)
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
            sources.push(Substream(id, n))
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
