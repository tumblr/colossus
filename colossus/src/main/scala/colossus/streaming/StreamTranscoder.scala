package colossus.streaming

import colossus.controller.{ControllerDownstream, ControllerUpstream, Encoding}
import colossus.core.{DisconnectCause, DownstreamEventHandler, UpstreamEventHandler}

/**
  * A Transcoder is used to convert streams of one encoding to streams of
  * another.  The two streams are intended to be part of a duplex pipeline, so
  * input is transcoded from A to B, and output is transcoded the other way B to
  * A
  */
trait Transcoder[U <: Encoding, D <: Encoding] {

  // this seems to be a bug in the compiler, but these type aliases are mandatory
  // to make this code compile
  type UI = U#Input
  type UO = U#Output
  type DI = D#Input
  type DO = D#Output

  def transcodeInput(source: Source[UI]): Source[DI]
  def transcodeOutput(source: Source[DO]): Source[UO]
}

/**
  * This controller interface can be used to transcode from one encoding to
  * another in a connection handler pipeline
  */
abstract class StreamTranscodingController[
    U <: Encoding,
    D <: Encoding
](
    val downstream: ControllerDownstream[D],
    transcoder: Transcoder[U, D]
) extends ControllerDownstream[U]
    with DownstreamEventHandler[ControllerDownstream[D]]
    with ControllerUpstream[D]
    with UpstreamEventHandler[ControllerUpstream[U]] {

  downstream.setUpstream(this)
  def namespace = downstream.namespace

  type UI = U#Input
  type UO = U#Output
  type DI = D#Input
  type DO = D#Output

  def outputStream: Pipe[DO, UO] = {
    val p = new BufferedPipe[DO](100)
    new Channel[DO, UO](p, transcoder.transcodeOutput(p))
  }

  val outgoing = new PipeCircuitBreaker[DO, UO](fatalError)

  def inputStream: Pipe[UI, DI] = {
    val p = new BufferedPipe[UI](100)
    new Channel(p, transcoder.transcodeInput(p))
  }

  val incoming = new PipeCircuitBreaker[UI, DI](fatalError)

  override def onConnected() {

    outgoing.set(outputStream)
    outgoing.into(upstream.outgoing) //, true, true){e => fatal(e)}

    incoming.set(inputStream)
    incoming.into(downstream.incoming) //, true, true){e => fatal(e)}
  }

  override def onConnectionTerminated(reason: DisconnectCause) {
    outgoing.unset()
    incoming.unset()
  }

  protected def fatal(state: NonOpenTransportState): Unit = state match {
    case TransportState.Closed             => upstream.connection.kill(new Exception("Stream closed unexpectedly"))
    case TransportState.Terminated(reason) => upstream.connection.kill(new Exception("Fatal Stream Error", reason))
  }

  protected def fatalError(err: Throwable) {
    upstream.connection.kill(new Exception("Fatal Stream Error", err))

  }

  def controllerConfig: colossus.controller.ControllerConfig = downstream.controllerConfig

  def connection: colossus.core.ConnectionManager = upstream.connection

}
