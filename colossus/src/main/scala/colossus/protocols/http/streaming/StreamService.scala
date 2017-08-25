package colossus.protocols.http.streaming

import colossus.controller.{ControllerDownstream, Encoding, FatalErrorAction}
import colossus.protocols.http.{HttpMessageHead, HttpRequestHead, HttpResponseHead}
import colossus.service.Protocol
import colossus.streaming._

import scala.language.higherKinds

object GenEncoding {

  type StreamHeader = Protocol {
    type Request  = HttpRequestHead
    type Response = HttpResponseHead
  }

  type HeadEncoding = Encoding {
    type Input <: HttpMessageHead
    type Output <: HttpMessageHead
  }

  //the difference between GenEncoding and ExEncoding is that Gen simply
  //retricts the types while Ex requires them to be exactly the type.  We use
  //Gen so that an encoding using StreamingHttpRequest can satisfy a requirement
  //of StreamingHttpMessage[HttpRequestHead].  We use Ex for HttpStream because
  //HttpStream[HttpRequest/ResponseHead] is the return type of the collapse
  //method, and if we used GenEncoding we'd have to make that return type
  //parameterized to match.

  type GenEncoding[M[T <: HttpMessageHead], E <: HeadEncoding] = Encoding {
    type Input <: M[E#Input]
    type Output <: M[E#Output]
  }

  type ExEncoding[M[T <: HttpMessageHead], E <: HeadEncoding] = Encoding {
    type Input  = M[E#Input]
    type Output = M[E#Output]
  }

  type InputMessageBuilder[H <: HttpMessageHead, T <: StreamingHttpMessage[H]] = (H, Source[Data]) => T

  implicit def ms[T <: HttpMessageHead]: MultiStream[Unit, HttpStream[T]] = new MultiStream[Unit, HttpStream[T]] {
    def component(c: HttpStream[T]) = c match {
      case Head(_)    => StreamComponent.Head
      case Data(_, _) => StreamComponent.Body
      case End        => StreamComponent.Tail
    }

    def streamId(c: HttpStream[T]) = ()
  }

}

import GenEncoding._

/**
  * This converts a raw http stream into a stream of http messages.  The type
  * parameters allow us to use this both for server streams and client streams
  */
abstract class HttpTranscoder[
    E <: HeadEncoding,
    A <: ExEncoding[HttpStream, E],
    B <: GenEncoding[StreamingHttpMessage, E]
] extends Transcoder[A, B] {

  // this has to be a member and not a constructor parameter because this only
  // compiles when using the type aliases, probably a compiler bug
  val builder: (E#Input, Source[Data]) => DI

  def transcodeInput(source: Source[UI]): Source[DI] = Multiplexing.demultiplex(source).map {
    case SubSource(id, stream) =>
      val head = stream.pull match {
        case PullResult.Item(Head(head)) => head
        case other                       => throw new Exception("not a head")
      }
      val mapped: Source[Data] = stream.filterMap {
        case d @ Data(_, _) => Some(d)
        case other          => None
      }
      builder(head, mapped)
  }
  def transcodeOutput(source: Source[DO]): Source[UO] = Source.flatten(source.map { _.collapse })
}

class HttpServerTranscoder
    extends HttpTranscoder[Encoding.Server[StreamHeader], Encoding.Server[StreamHttp], Encoding.Server[StreamingHttp]] {
  val builder = StreamingHttpRequest.apply _
}

class HttpClientTranscoder
    extends HttpTranscoder[Encoding.Client[StreamHeader], Encoding.Client[StreamHttp], Encoding.Client[StreamingHttp]] {
  val builder = StreamingHttpResponse.apply _: (HttpResponseHead, Source[Data]) => DI
}

abstract class HttpStreamController[
    E <: HeadEncoding,
    A <: ExEncoding[HttpStream, E],
    B <: GenEncoding[StreamingHttpMessage, E]
](ds: ControllerDownstream[B], transcoder: HttpTranscoder[E, A, B])
    extends StreamTranscodingController[A, B](ds, transcoder)

class HttpStreamServerController(ds: ControllerDownstream[Encoding.Server[StreamingHttp]])
    extends HttpStreamController[Encoding.Server[StreamHeader],
                                 Encoding.Server[StreamHttp],
                                 Encoding.Server[StreamingHttp]](ds, new HttpServerTranscoder) {

  def onFatalError(reason: Throwable) = FatalErrorAction.Disconnect(None)
}

class HttpStreamClientController(ds: ControllerDownstream[Encoding.Client[StreamingHttp]])
    extends HttpStreamController[Encoding.Client[StreamHeader],
                                 Encoding.Client[StreamHttp],
                                 Encoding.Client[StreamingHttp]](ds, new HttpClientTranscoder) {

  def onFatalError(reason: Throwable) = FatalErrorAction.Terminate

}
