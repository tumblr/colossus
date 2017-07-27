package colossus
package streaming

trait MultiStream[K, T] extends Stream[T] {
  def streamId(t: T): K
}

case class SubSource[K,T](id: K, stream: Source[T])
case class SubSink[K,T](id: K, stream: Sink[T])

  /**
   * Multiplexing is the process of combining multiple independant streams into
   * a single stream.  Each message in the multiplexed stream carries
   * information about which sub-stream it originated from, so that eventually
   * the stream can be demultiplexed back into the constituent sub-streams.
   *
   * By multiplexing a "base" `Sink[T]`, a new `Sink[SubSource[K,T]]` is
   * created, with each [[SubSource]] containing a `Source[T]`.  When a
   * [[Source]] is pushed into the multiplexing sink, all messages pushed to
   * that source are routed into the base sink.
   *
   * Likewise, demultiplexing a multiplexed `Source[T]` will create a
   * `Source[SubSource[K,T]]`, with each [[Subsource]] being one of the
   * sub-streams being fed into the multiplexed source.
   */
object Multiplexing {

  /**
   * Multiplex a base [[Sink]] into a multiplexed `Sink[SubSource[K,T]]`.  See
   * the docs on [[Multiplexing]] for more details.
   *
   * @param base The sink to multiplex
   * @return A multiplexing Sink that can accept new sources to route into the base sink
   */
  def multiplex[K,T](base: Sink[T])(implicit ms: MultiStream[K,T]): Sink[SubSource[K,T]] = {
    val sources = new BufferedPipe[SubSource[K,T]](1) //backpressure never an issue with this pipe
    var active = Map[K, Source[T]]()
    def onSubClosed(id: K) {
      active  = active - id
      if (active.size == 0 && sources.outputState == TransportState.Closed) {
        base.complete()
      }
    }
    def fatal(message: String): PullAction = {
      val exception = new PipeStateException(message)
      base.terminate(exception)
      sources.terminate(exception)
      active.foreach{case (_, sub) => sub.terminate(exception)}
      PullAction.Stop
    }
    sources.pullWhile (
      sub => {
        active = active + (sub.id -> sub.stream)
        sub.stream.into(base, false, false){
          case TransportState.Terminated(err) if (base.inputState != TransportState.Open) => {
            //when this occurs it means the base sink was terminated.  At this
            //point only this subsource knows about that, so we'll call fatal
            //and kill everything
            fatal(s"Multiplexed Stream Terminated: $err")
          }
          case other => {
            onSubClosed(sub.id)
          }
        }
        PullAction.PullContinue
      }, {
        case PullResult.Closed => {
          if (active.size == 0) {
            base.complete()
          }
        }
        case PullResult.Error(err) => fatal(s"Multiplexed Sink Terminated: $err")
      }
    )
    sources
  }
                
  
  /**
   * Demultiplex a multiplexed stream into individual sub-streams.  An implicit
   * [[Multistream[K,T] Multistream]] is required in order to determine the
   * proper substream of each message in the multiplexed stream.
   *
   * @param base The multiplexed source
   * @param sourcesBufferSize the internal buffer size of the stream of sources
   * @param substreamBufferSize the internal buffer size of each newly created substream
   * @return A Source of substreams
   */
  def demultiplex[K,T](base: Source[T], sourcesBufferSize: Int = 10, substreamBufferSize: Int = 10)(implicit ms: MultiStream[K,T]): Source[SubSource[K,T]] = {
    var active = Map[K, Pipe[T,T]]()
    val sources = new BufferedPipe[SubSource[K,T]](sourcesBufferSize)
    def fatal(reason: String): PullAction = {
      base.terminate(new PipeStateException(reason))
      sources.terminate(new PipeStateException(reason))
      active.foreach{case (_, sink) => sink.terminate(new PipeStateException(reason))}
      PullAction.Stop
    }
    def doPull(): Unit = base.pullWhile (
      item => {
        val id = ms.streamId(item)
        def tryPush(close: Boolean): PullAction = if (active contains id) {
          active(id).push(item) match {
            case PushResult.Full(signal) => PullAction.Wait(signal)
            case PushResult.Closed => fatal(s"sub-stream $id unexpectedly closed")
            case PushResult.Error(error) => fatal(s"error in substream $id: $error")
            case other => {
              if (close) {
                active(id).complete()
                active = active - id
              }
              PullAction.PullContinue
            }
          }
        } else {
          fatal(s"tried to push to non-existant stream $id")
        }
        ms.component(item) match {
          case StreamComponent.Head => if (active contains id) {
            fatal(s"duplicate id $id")
          } else {
            val n = new BufferedPipe[T](substreamBufferSize)
            n.push(item)
            //TODO sources backpressure
            active = active + (id -> n)
            sources.push(SubSource(id, n))
            PullAction.PullContinue
          }
          case StreamComponent.Body => tryPush(false)
          case StreamComponent.Tail => tryPush(true)
        }
      }, {
        case PullResult.Closed => {
          sources.complete()
          active.foreach{ case (_, sub) => sub.terminate(new PipeStateException("multiplexed stream closed"))}
        }
        case PullResult.Error(error) => fatal(s"Source terminated with error: $error")
      }
    )
    doPull()
    sources
  }
}
