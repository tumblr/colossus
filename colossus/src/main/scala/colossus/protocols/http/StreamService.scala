package colossus
package protocols.http
package stream

import streaming._
import controller._
import service.Protocol
import core._
import service._

import scala.language.higherKinds
import scala.util.{Try, Success, Failure}

trait StreamingHttpMessage[T <: HttpMessageHead] {

  def head: T
  def body: Source[Data]

  def collapse: Source[HttpStream[T]] = Source.one[HttpStream[T]](Head(head)) ++ body ++ Source.one[HttpStream[T]](End)
}

case class StreamingHttpRequest(head: HttpRequestHead, body: Source[Data]) extends StreamingHttpMessage[HttpRequestHead]


case class StreamingHttpResponse(head: HttpResponseHead, body: Source[Data]) extends StreamingHttpMessage[HttpResponseHead]
object StreamingHttpResponse {
  def apply(response: HttpResponse) = new StreamingHttpMessage[HttpResponseHead] {
    def head = response.head
    def body = Source.one(Data(response.body.asDataBlock))

    override def collapse = Source.fromArray(Array(Head(head), Data(response.body.asDataBlock), End))
  }
}


trait StreamingHttp extends Protocol {
  type Request = StreamingHttpMessage[HttpRequestHead]
  type Response = StreamingHttpMessage[HttpResponseHead]
}


object GenEncoding {

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

}
import GenEncoding._


trait InputMessageBuilder[T <: HttpMessageHead] {
  def build(head: T): (Sink[Data], StreamingHttpMessage[T])
}
object StreamRequestBuilder extends InputMessageBuilder[HttpRequestHead] {
  def build(head: HttpRequestHead) = {
    val pipe = new BufferedPipe[Data](10)
    (pipe, StreamingHttpRequest(head, pipe))
  }
}

class StreamServiceController[E <: HeadEncoding](
  val downstream: ControllerDownstream[GenEncoding[StreamingHttpMessage, E]],
  builder: InputMessageBuilder[E#Input]
)
extends ControllerDownstream[GenEncoding[HttpStream, E]] 
with DownstreamEventHandler[ControllerDownstream[GenEncoding[StreamingHttpMessage, E]]] 
with ControllerUpstream[GenEncoding[StreamingHttpMessage, E]]
with UpstreamEventHandler[ControllerUpstream[GenEncoding[HttpStream, E]]] {

  downstream.setUpstream(this)

  type InputHead = E#Input
  type OutputHead = E#Output

  private var currentInputStream: Option[Sink[Data]] = None

  def outputStream: Pipe[Source[HttpStream[OutputHead]], HttpStream[OutputHead]] = {
    val p = new BufferedPipe[Source[HttpStream[OutputHead]]](100)
    new Channel(p, Source.flatten(p))
  }

  val pipe = new PipeCircuitBreaker[Source[HttpStream[OutputHead]], HttpStream[OutputHead]]
  
  //setup routing the pipe into upstream
  pipe.pullWhile {
    case PullResult.Item(item) => {
      upstream.pushFrom(item, 0, _ => ())
      true
    }
    case other => {
      fatal(s"Unexpected streaming item $other")
      false
    }
  }

  override def onConnected() {
    pipe.set(outputStream)
  }

  override def onConnectionTerminated(reason: DisconnectCause) {
    pipe.unset().foreach{_.terminate(new ConnectionLostException("Closed"))}
  }


  protected def fatal(message: String) {
    println(s"FATAL ERROR: $message")
    upstream.connection.forceDisconnect()
  }

  def processMessage(input: HttpStream[InputHead]) {
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
      case b @ Data(_, _) => currentInputStream match {
        case Some(sink) => sink.push(b) match {
          case PushResult.Full(signal) => {
            upstream.pauseReads()
            signal.notify{
              sink.push(b)
              upstream.resumeReads()
            }
          }
          case PushResult.Ok => {}
          case other => {
            // :(
            fatal(s"failed to push message to stream with result $other")
          }
        }
        case None => {
          fatal("Received body data but no input stream exists")
        }
      }
      case e @ End => currentInputStream match {
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
  def controllerConfig: colossus.controller.ControllerConfig = downstream.controllerConfig

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
  def canPush: Boolean = true

  def pushFrom(item: GenEncoding[StreamingHttpMessage,E]#Output,createdMillis: Long, postWrite: QueuedItem.PostWrite): Boolean = {
    val source = item.collapse
    val res = pipe push source 
    res match {
      case PushResult.Ok => true
      case _ => false
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
      new Controller[GenEncoding[HttpStream, Encoding.Server[StreamHeader]]](
        new StreamServiceController[Encoding.Server[StreamHeader]](
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
