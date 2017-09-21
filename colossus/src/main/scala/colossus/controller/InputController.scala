package colossus.controller

import colossus.core.{AliveState, DataBuffer}
import colossus.metrics.collectors.Histogram
import colossus.parsing.ParserSizeTracker
import colossus.streaming.{PushResult, Source}

//TODO: the onComplete/onError logic should be moved out of here and instead use
//the callback passed to Source.into.  However, that requires having an iterator
//that can return an error
abstract class CodecBufferIterator[E <: Encoding](
    codec: Codec[E],
    buffer: DataBuffer
) extends Iterator[E#Input] {

  def onComplete()
  def onError(reason: Throwable)

  var done = false

  def maybeNext(): Option[E#Input] =
    try {
      val n = codec.decode(buffer)
      if (n.isEmpty) {
        onComplete()
      }
      n
    } catch {
      case t: Throwable => {
        onError(t)
        None
      }
    }

  var nextItem = maybeNext()

  def hasNext = nextItem.isDefined
  def next() = {
    val n = nextItem.get
    nextItem = maybeNext()
    n
  }
}

trait StaticInputController[E <: Encoding] extends BaseController[E] {
  private var _readsEnabled = true
  def readsEnabled          = _readsEnabled

  //this has to be lazy to avoid initialization-order NPE
  lazy val inputSizeHistogram = if (controllerConfig.metricsEnabled) {
    Some(Histogram("input_size", sampleRate = 0.10, percentiles = List(0.75, 0.99)))
  } else {
    None
  }
  lazy val inputSizeTracker = new ParserSizeTracker(Some(controllerConfig.inputMaxSize), inputSizeHistogram)

  def pauseReads() {
    upstream.connectionState match {
      case a: AliveState => {
        _readsEnabled = false
        a.endpoint.disableReads()
      }
      case _ => {}
    }
  }

  def resumeReads() {
    upstream.connectionState match {
      case a: AliveState => {
        _readsEnabled = true
        a.endpoint.enableReads()
      }
      case _ => {}
    }
  }

  def receivedData(data: DataBuffer) {
    try {
      while (inputSizeTracker.track(data)(codec.decode(data)) match {
               case Some(msg) =>
                 incoming.push(msg) match {
                   case PushResult.Ok => true
                   case PushResult.Full(signal) => {
                     pauseReads()
                     (Source.one(msg) ++ Source.fromIterator(new CodecBufferIterator(codec, data.takeCopy) {
                       def onComplete() { resumeReads() }
                       def onError(reason: Throwable) { fatalError(reason, false) }
                     })).into(incoming, linkClosed = false, linkTerminated = false)(_ => {})
                     false
                   }
                   case PushResult.Error(reason) => {
                     fatalError(reason, false)
                     false
                   }
                   case PushResult.Closed => {
                     fatalError(new Exception("attempted to push to closed pipe"), false)
                     false
                   }
                 }
               case None => false
             }) {}
    } catch {
      case reason: Throwable => {
        fatalError(reason, false)
      }
    }
  }

  override def onConnected() {
    super.onConnected()
    codec.reset()
  }

}
