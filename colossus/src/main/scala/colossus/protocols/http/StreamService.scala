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

trait StreamingHttpMessage[T <: HttpMessageHead] extends BaseHttpMessage[T, Source[Data]] {

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

  type InputMessageBuilder[T <: HttpMessageHead] = (T, Source[Data]) => StreamingHttpMessage[T]

}
import GenEncoding._


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

  implicit val ms:MultiStream[Unit, HttpStream[InputHead]] = new MultiStream[Unit, HttpStream[InputHead]] {
    def component(c: HttpStream[InputHead]) = c match {
      case Head(_) => StreamComponent.Head
      case Data(_,_) => StreamComponent.Body
      case End   => StreamComponent.Tail
    }

    def streamId(c: HttpStream[InputHead]) = ()
  }



  def outputStream: Pipe[StreamingHttpMessage[OutputHead], HttpStream[OutputHead]] = {
    val p = new BufferedPipe[StreamingHttpMessage[OutputHead]](100).map{_.collapse}
    new Channel(p, Source.flatten(p))
  }

  val outgoing = new PipeCircuitBreaker[StreamingHttpMessage[OutputHead], HttpStream[OutputHead]]


  def inputStream : Pipe[HttpStream[InputHead], StreamingHttpMessage[InputHead]] = {
    val p = new BufferedPipe[HttpStream[InputHead]](100)
    val demult = Multiplexing.demultiplex(p).map{ case SubSource(id, stream) =>
      val head = stream.pull match {
        case PullResult.Item(Head(head)) => head
        case other => throw new Exception("not a head")
      }
      val mapped : Source[Data] = stream.filterMap{
        case d @ Data(_,_) => Some(d)
        case other => None
      }
      builder(head, mapped)
    }
    new Channel(p, demult)
  }

  val incoming = new PipeCircuitBreaker[HttpStream[InputHead], StreamingHttpMessage[InputHead]]

    

  override def onConnected() {
    
    outgoing.set(outputStream)
    outgoing.into(upstream.outgoing, true, true){e => fatal(e.toString)}

    incoming.set(inputStream)
    incoming.into(downstream.incoming, true, true){e => fatal(e.toString)}
  }

  override def onConnectionTerminated(reason: DisconnectCause) {
    outgoing.unset()//.foreach{_.terminate(new ConnectionLostException("Closed"))}
    incoming.unset()
  }


  protected def fatal(message: String) {
    println(s"FATAL ERROR: $message")
    upstream.connection.forceDisconnect()
  }

  def controllerConfig: colossus.controller.ControllerConfig = downstream.controllerConfig

  def connection: colossus.core.ConnectionManager = upstream.connection

}


class StreamingHttpServiceHandler(rh: GenRequestHandler[StreamingHttp]) 
extends ServiceServer[StreamingHttp](rh) {

}


class StreamServiceHandlerGenerator(ctx: InitContext) extends HandlerGenerator[GenRequestHandler[StreamingHttp]](ctx) {
  
  def fullHandler = handler => {
    new PipelineHandler(
      new Controller[GenEncoding[HttpStream, Encoding.Server[StreamHeader]]](
        new StreamServiceController[Encoding.Server[StreamHeader]](
          new StreamingHttpServiceHandler(handler),
          StreamingHttpRequest.apply
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

