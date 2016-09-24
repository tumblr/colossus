package colossus
package protocols.http
package stream

import controller._
import service.Protocol
import core._
import service._

import scala.language.higherKinds
import scala.util.{Try, Success, Failure}

trait StreamingHttpMessage[T <: HttpMessageHead] {

  def head: T
  def body: Source[BodyData[T]]

  def collapse: Source[StreamHttpMessage[T]] = Source.one[StreamHttpMessage[T]](Head(head)) ++ body
}

case class StreamingHttpRequest(head: HttpRequestHead, body: Source[BodyData[HttpRequestHead]]) extends StreamingHttpMessage[HttpRequestHead]
case class StreamingHttpResponse(head: HttpResponseHead, body: Source[BodyData[HttpResponseHead]]) extends StreamingHttpMessage[HttpResponseHead]




object GenEncoding {
  trait StreamingHttp extends Protocol {
    type Request = StreamingHttpMessage[HttpRequestHead]
    type Response = StreamingHttpMessage[HttpResponseHead]
  }

  type StreamHeader = Protocol {
    type Request = HttpRequestHead
    type Response = HttpResponseHead
  }
  type HeadEncoding = Encoding {
    type Input <: HttpMessageHead
    type  Output <: HttpMessageHead
  }

  type GenEncoding[M[T <: HttpMessageHead], E <: HeadEncoding] = Encoding {
    type Input = M[E#Input]
    type Output = M[E#Output]
  }

  type HttpHeadProtocol = Protocol {
    type Request = HttpRequestHead
    type Response = HttpResponseHead
  }

  //type StreamingEncoding = Encoding { type Input <: StreamingHttpMessage
}
import GenEncoding._


trait InputMessageBuilder[T <: HttpMessageHead] {
  def build(head: T): (Sink[BodyData[T]], StreamingHttpMessage[T])
}
object StreamRequestBuilder extends InputMessageBuilder[HttpRequestHead] {
  def build(head: HttpRequestHead) = {
    val pipe = new BufferedPipe[BodyData[HttpRequestHead]](10)
    (pipe, StreamingHttpRequest(head, pipe))
  }
}

class StreamServiceServerController[E <: GenEncoding.HeadEncoding](
  val downstream: ControllerDownstream[GenEncoding[StreamingHttpMessage, E]],
  builder: InputMessageBuilder[E#Input]
)
extends ControllerDownstream[GenEncoding[StreamHttpMessage, E]] 
with ControllerUpstream[GenEncoding[StreamingHttpMessage, E]]
with DownstreamEventHandler[ControllerDownstream[GenEncoding[StreamHttpMessage, E]]] 
with UpstreamEventHandler[ControllerUpstream[GenEncoding[StreamingHttpMessage, E]]] {

  downstream.setUpstream(this)

  type InputHead = E#Input
  type OutputHead = E#Output

  private var currentInputStream: Option[Sink[BodyData[InputHead]]] = None

  private val outputStreams = new MessageQueue[Source[StreamHttpMessage[OutputHead]]](100)

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
  def pauseReads(): Unit = upstream.pauseReads()
  def pauseWrites(): Unit = upstream.pauseWrites()
  def pendingBufferSize: Int = ???
  def purgePending(reason: Throwable): Unit = ???
  def resumeReads(): Unit = upstream.resumeReads()
  def resumeWrites(): Unit = upstream.resumeWrites()
  def writesEnabled: Boolean = upstream.writesEnabled

  // Members declared in colossus.controller.Writer
  def canPush: Boolean = !outputStreams.isFull

  def pushFrom(item: GenEncoding[StreamingHttpMessage,E]#Output,createdMillis: Long, postWrite: QueuedItem.PostWrite): Boolean = {
    if (canPush) {
      val source = item.collapse
      outputStreams.enqueue(source, postWrite, createdMillis)
      if (outputStreams.size == 1) {
        drain(source)
      }
      true
    } else false
  }

  def drain(source: Source[StreamHttpMessage[OutputHead]]) {
    source.pull{
      case Success(Some(item)) => {
        //TODO:
        upstream.push(item)(_ => ())
        drain(source)
      }
      case Success(None) => {
        val done = outputStreams.dequeue
        done.postWrite(OutputResult.Success)
        if (!outputStreams.isEmpty) {
          drain(outputStreams.head.item)
        }
      }
      case Failure(reason) => {
        fatal(s"Error writing stream: $reason")
      }
    }
  }

  

}


class StreamingHttpServiceHandler(rh: GenRequestHandler[StreamingHttp]) 
extends DSLService[StreamingHttp](rh) {

  /*
  val defaults = new Http.ServerDefaults

  override def tagDecorator = new ReturnCodeTagDecorator

  override def processRequest(input: Http#Input): Callback[Http#Output] = {
    val response = super.processRequest(input)
    if(!input.head.persistConnection) connection.disconnect()
    response
  }
  */
  def unhandledError = {
    case error => ???//defaults.errorResponse(error)
  }

}


class StreamServiceHandlerGenerator(ctx: InitContext) extends HandlerGenerator[GenRequestHandler[StreamingHttp]](ctx) {
  
  def fullHandler = handler => {
    new PipelineHandler(
      new Controller[GenEncoding[StreamHttpMessage, StreamHeader#ServerEncoding]](
        new StreamServiceServerController[StreamHeader#ServerEncoding](
          new StreamingHttpServiceHandler(handler),
          StreamRequestBuilder
        ),
        new StreamHttpServerCodec
      ), 
      handler
    ) with ServerConnectionHandler
  }
}

abstract class StreamServiceInitializer(ctx: InitContext) extends StreamServiceHandlerGenerator(ctx) with ServiceInitializer[GenRequestHandler[StreamingHttp]] 

object StreamHttpServiceServer extends ServiceDSL[GenRequestHandler[StreamingHttp], StreamServiceInitializer] {
  def basicInitializer = new StreamServiceHandlerGenerator(_)
}
