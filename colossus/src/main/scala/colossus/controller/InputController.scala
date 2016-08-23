package colossus
package controller

import colossus.metrics.Histogram
import colossus.parsing.ParserSizeTracker
import core._
import colossus.service.NotConnectedException

//TODO : pausing reads should immediately stop calls to processMessage, so we
//need to copy and hold remaining data in the databuffer and drain it when
//resume is called
trait StaticInputController[E <: Encoding] extends BaseController[E] {
  private var _readsEnabled = true
  def readsEnabled = _readsEnabled

  //this has to be lazy to avoid initialization-order NPE
  lazy val inputSizeHistogram = if (controllerConfig.metricsEnabled) {
    Some(Histogram("input_size", sampleRate = 0.10, percentiles = List(0.75,0.99)))
  } else {
    None
  }
  lazy val inputSizeTracker = new ParserSizeTracker(Some(controllerConfig.inputMaxSize), inputSizeHistogram)

  def pauseReads() {
    upstream.connectionState match {
      case a : AliveState => {
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
    println("DATA")
    try {
      var done = false
      while (!done) {
        inputSizeTracker.track(data)(codec.decode(data)) match {
          case Some(msg) => downstream.processMessage(msg)
          case None => done = true
        }
      }
    } catch {
      case reason: Throwable => {
        fatalError(reason)
      }
    }
  }

  override def onConnected() {
    super.onConnected()
    codec.reset()
  }

}
