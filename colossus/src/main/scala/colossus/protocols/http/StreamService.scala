package colossus
package protocols.http
package stream

import controller._
import service.Protocol
import core._

import scala.language.higherKinds

trait StreamingHttpMessage[T <: HttpMessageHead] {

  def head: T
  def body: Source[BodyData[T]]

  def collapse: Source[StreamHttpMessage[T]] = Source.one[StreamHttpMessage[T]](Head(head)) ++ body
}

case class StreamingHttpRequest(head: HttpRequestHead, body: Source[BodyData[HttpRequestHead]]) extends StreamingHttpMessage[HttpRequestHead]
case class StreamingHttpResponse(head: HttpResponseHead, body: Source[BodyData[HttpResponseHead]]) extends StreamingHttpMessage[HttpResponseHead]


trait StreamingHttp extends Protocol {
  type Request = StreamingHttpRequest
  type Response = StreamingHttpResponse
}

object GenEncoding {
  trait HeadEncoding extends Encoding {
    type Input <: HttpMessageHead
    type  Output <: HttpMessageHead
  }

  type GenEncoding[M[T <: HttpMessageHead], E <: HeadEncoding] = Encoding {
    type Input = M[E#Input]
    type Output = M[E#Output]
  }
}
import GenEncoding._

trait HttpHeadProtocol extends Protocol {
  type Request = HttpRequestHead
  type Response = HttpResponseHead
}

trait InputMessageBuilder[T <: HttpMessageHead] {
  def build(head: T): (Sink[BodyData[T]], StreamingHttpMessage[T])
}

class StreamServiceServerController[E <: GenEncoding.HeadEncoding](
  val downstream: ControllerDownstream[GenEncoding[StreamingHttpMessage, E]],
  builder: InputMessageBuilder[E#Input]
)
extends ControllerDownstream[GenEncoding[StreamHttpMessage, E]] 
with ControllerUpstream[GenEncoding[StreamingHttpMessage, E]]
with DownstreamEventHandler[ControllerDownstream[GenEncoding[StreamingHttpMessage, E]]] 
with UpstreamEventHandler[ControllerUpstream[GenEncoding[StreamHttpMessage, E]]] {

  downstream.setUpstream(this)

  type InputHead = E#Input
  type OutputHead = E#Output

  private var currentInputStream: Option[Sink[BodyData[InputHead]]] = None

  protected def fatal(message: String) {
    println(s"FATAL ERROR: $message")
    upstream.connection.forceDisconnect()
  }

  def processMessage(input: StreamHttpMessage[InputHead]) {
    input match {
      case Head(head) => currentInputStream match {
        case None => {
          val (sink, msg) = builder.build(head)
          currentInputStream = Some(sink)
          downstream.processMessage(msg)
        }
        case Some(uhoh) => {
          //we got a head before the last stream finished, not good
          fatal("received head during unfinished stream")
        }
      }
      case b @ BodyData(_, _) => currentInputStream match {
        case Some(sink) => sink.push(b) match {
          case PushResult.Filled(signal) => {
            upstream.pauseReads()
            signal.react{
              upstream.resumeReads()
            }
          }
          case bad: PushResult.NotPushed => {
            // :(
            fatal("failed to push message to stream")
          }
          case other => {}
        }
        case None => {
          fatal("Received body data but no input stream exists")
        }
      }
      case e @ End() => currentInputStream match {
        case Some(sink) => {
          sink.complete()
          currentInputStream = None
        }
        case None => {
          fatal("attempted to end non-existant input stream")
        }
      }
    }
  }

  // Members declared in colossus.controller.ControllerDownstream
  def controllerConfig: colossus.controller.ControllerConfig = ???

  // Members declared in colossus.controller.ControllerUpstream
  def connection: colossus.core.ConnectionManager = upstream.connection
  def pauseReads(): Unit = ???
  def pauseWrites(): Unit = ???
  def pendingBufferSize: Int = ???
  def purgePending(reason: Throwable): Unit = ???
  def resumeReads(): Unit = ???
  def resumeWrites(): Unit = ???
  def writesEnabled: Boolean = ???

  // Members declared in colossus.controller.Writer
  def canPush: Boolean = ???
  def pushFrom(item: colossus.protocols.http.stream.GenEncoding.GenEncoding[colossus.protocols.http.stream.StreamingHttpMessage,E]#Output,createdMillis: Long,postWrite: colossus.controller.QueuedItem.PostWrite): Boolean = ???
  

}
