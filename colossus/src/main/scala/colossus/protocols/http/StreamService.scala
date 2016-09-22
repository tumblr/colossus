package colossus
package protocols.http
package stream

import controller._
import service.Protocol
import core._

import scala.language.higherKinds

trait StreamingHttpMessage[T <: HttpMessageHead[T]] {

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
    type Input <: HttpMessageHead[Input]
    type  Output <: HttpMessageHead[Output]
  }

  type GenEncoding[M[T <: HttpMessageHead[T]], E <: HeadEncoding] = Encoding {
    type Input = M[E#Input]
    type Output = M[E#Output]
  }
}
import GenEncoding._

trait HttpHeadProtocol extends Protocol {
  type Request = HttpRequestHead
  type Response = HttpResponseHead
}

trait InputMessageBuilder[T <: HttpMessageHead[T]] {
  def build(head: T): (Sink[BodyData[T]], StreamingHttpMessage[T])
}

class StreamServiceServerController[E <: GenEncoding.HeadEncoding](builder: InputMessageBuilder[E#Input])
extends ControllerDownstream[GenEncoding[StreamHttpMessage, E]] with ControllerUpstream[GenEncoding[StreamingHttpMessage, E]]
with DownstreamEventHandler[ControllerDownstream[GenEncoding[StreamingHttpMessage, E]]] 
with UpstreamEventHandler[ControllerUpstream[GenEncoding[StreamHttpMessage, E]]] {


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
  

}
