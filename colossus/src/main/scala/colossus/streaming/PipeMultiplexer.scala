package colossus
package streaming

import controller._

trait MultiStream[K, T] extends Stream[T] {
  def streamId(t: T): K
}

case class SubSource[K,T](id: K, stream: Source[T])
case class SubSink[K,T](id: K, stream: Sink[T])

object Multiplexing {

  def multiplex[K,T](base: Sink[T])(implicit ms: MultiStream[K,T]): Sink[SubSource[K,T]] = {
    val sources = new BufferedPipe[SubSource[K,T]](10)
    var active = Map[K, Source[T]]()
    def onSubClosed(id: K) {
      active  = active - id
      if (active.size == 0 && sources.outputState == TransportState.Closed) {
        base.complete()
      }
    }
    sources.pullWhile {
      case PullResult.Item(sub) => {
        active = active + (sub.id -> sub.stream)
        def fatal(message: String): Boolean = {
          sub.stream.terminate(new PipeStateException(message))
          false
        }
        def doPull(): Unit = sub.stream.pullWhile{
          case PullResult.Item(item) => base.push(item) match {
            case PushResult.Full(sig) => {
              sig.notify{
                base.push(item) match {
                  case PushResult.Ok => doPull()
                  case PushResult.Filled(_) => doPull() //TODO: ignoring Filled makes sense here
                  case other => {println(s"oops: $other");fatal(s"Unexpected PushResult while multiplexing $other")}
                }
              }
              false
            }
            case PushResult.Closed => fatal(s"Multiplexed base stream unexpectedly closed")
            case PushResult.Error(err)  => fatal(s"Failed to push to base stream: $err")
            case other => true
          }
          case other => {
            onSubClosed(sub.id)
            false
          }
        }
        doPull()
        true
      }
      case PullResult.Closed => {
        if (active.size == 0) {
          base.complete()
        }
        false
      }
      case PullResult.Error(err) => {
        base.terminate(new PipeStateException(s"Multiplexed Sink Terminated: $err"))
        active.foreach{case (id, source) => source.terminate(err)}
        false
      }
    }
    sources
  }
                
  
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
