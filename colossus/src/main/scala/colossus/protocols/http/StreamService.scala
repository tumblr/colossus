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


  def outputStream: Pipe[StreamingHttpMessage[OutputHead], HttpStream[OutputHead]] = {
    val p = new BufferedPipe[StreamingHttpMessage[OutputHead]](100).map{_.collapse}
    new Channel(p, Source.flatten(p))
  }

  val outgoing = new PipeCircuitBreaker[StreamingHttpMessage[OutputHead], HttpStream[OutputHead]]


  val incoming = new BufferedPipe[HttpStream[InputHead]](100)

  override def onConnected() {
    outgoing.set(outputStream)
    outgoing into upstream.outgoing
    readin()
  }

  override def onConnectionTerminated(reason: DisconnectCause) {
    outgoing.unset().foreach{_.terminate(new ConnectionLostException("Closed"))}
  }


  protected def fatal(message: String) {
    println(s"FATAL ERROR: $message")
    upstream.connection.forceDisconnect()
  }

  def readin(): Unit = incoming.pullWhile {
    case PullResult.Item(i) =>{ i match {
      case Head(head) =>  currentInputStream match {
        case None => {
          val (sink, msg) = builder.build(head)
          currentInputStream = Some(sink)
          downstream.incoming.push(msg)
        }
        case Some(uhoh) => {
          //we got a head before the last stream finished, not good
          fatal("received head during unfinished stream")
        }
      }
      case b @ Data(_, _) => currentInputStream match {
        case Some(sink) => sink.push(b) match {
          case PushResult.Full(signal) => {
            signal.notify{
              sink.push(b)
              readin()
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
    } ;true }
    case _ => ???
  }

  // Members declared in colossus.controller.ControllerDownstream
  def controllerConfig: colossus.controller.ControllerConfig = downstream.controllerConfig

  // Members declared in colossus.controller.ControllerUpstream
  def connection: colossus.core.ConnectionManager = upstream.connection

}


class StreamingHttpServiceHandler(rh: GenRequestHandler[StreamingHttp]) 
extends DSLService[StreamingHttp](rh) {

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

