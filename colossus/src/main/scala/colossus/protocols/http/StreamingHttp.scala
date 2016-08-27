package colossus
package protocols.http
package stream

import colossus.metrics.MetricNamespace
import controller._
import service.Protocol
import core._
import parsing.Combinators.Parser

class StreamHttpException(message: String) extends Exception(message)

sealed trait StreamHttpMessage[T <: HttpMessageHead[T]]

case class Head[T <: HttpMessageHead[T]](head: T) extends StreamHttpMessage[T]

sealed trait StreamBodyMessage[T <: HttpMessageHead[T]] extends StreamHttpMessage[T]
/**
 * A Piece of data for a http message body.  `chunkEncoded` declares whether the data
 * is raw data(false) or a properly encoded http chunk (true).  In most cases
 * this should be false unless the data is being proxied verbatim.
 */
case class BodyData[T <: HttpMessageHead[T]](data: DataBlock, chunkEncoded: Boolean = false) extends StreamBodyMessage[T]
case class End[T <: HttpMessageHead[T]]() extends StreamBodyMessage[T]


trait StreamHttp extends Protocol {


  type Request = StreamHttpMessage[HttpRequestHead]
  type Response = StreamHttpMessage[HttpResponseHead]
}

trait HeadParserProvider[T <: HttpMessageHead[T]] {
  def parser: Parser[T]
  def eosTerminatesMessage : Boolean
}
object HeadParserProvider {
  implicit object RequestHeadParserProvider extends HeadParserProvider[HttpRequestHead] {
    def parser = HttpRequestParser.httpHead

    def eosTerminatesMessage = false

    
  }
  object ResponseHeadParserProvider extends HeadParserProvider[HttpResponseHead] {
    def parser = HttpResponseParser.head

    def eosTerminatesMessage = true
  }
}

trait StreamDecoder[T <: HttpMessageHead[T]] { 

  def parserProvider: HeadParserProvider[T]

  private var headParser = parserProvider.parser

  private sealed trait State
  private case object HeadState extends State
  private sealed trait BodyState extends State {
    def nextPiece(input: DataBuffer): Option[StreamBodyMessage[T]]
  }
  //used when the transfer-encoding is identity or a content-length is provided
  //(so non-chunked encoding)
  private class FiniteBodyState(val size: Option[Int]) extends BodyState {
    private var taken = 0
    def done = size.map{_ == taken}.getOrElse(false)
    def nextPiece(input: DataBuffer): Option[StreamBodyMessage[T]] = if (done) {
      Some(End())
    } else if (input.hasUnreadData) {
      val bytes = input.take(size.map{_ - taken}.getOrElse(input.remaining))
      taken += bytes.length
      Some(BodyData(DataBlock(bytes), false))
    } else {
      None
    }
  }
  private class ChunkedBodyState extends BodyState {
    import parsing.Combinators._
    val parser: Parser[StreamBodyMessage[T]] = intUntil('\r', 16) <~ byte |> {
      case 0 => bytes(2) >> {_ => End()}
      case n => bytes(n.toInt) <~ bytes(2) >> {bytes => BodyData(DataBlock(bytes), false)}
    }
    def nextPiece(input: DataBuffer): Option[StreamBodyMessage[T]] = parser.parse(input)
  
  }
  private var state: State = HeadState

  def decode(data: DataBuffer): Option[StreamHttpMessage[T]] = state match {
    case HeadState => headParser.parse(data) match {
      case Some(h) => {
        if (h.headers.transferEncoding == TransferEncoding.Chunked) {
          state = new ChunkedBodyState
        } else {
          val lengthOpt = if (parserProvider.eosTerminatesMessage || h.headers.contentLength.isDefined) {
            h.headers.contentLength
          } else {
            Some(0)
          }
          state = new FiniteBodyState(lengthOpt)
        }
        Some(Head(h))
      }
      case None => None
    }
    case b: BodyState => b.nextPiece(data) match {
      case Some(End()) => {
        state = HeadState
        Some(End())
      }
      case other => other
    }
  }


  def resetDecoder() {
    state = HeadState
    headParser = parserProvider.parser
  }

  
  def endOfStream(): Option[StreamHttpMessage[T]] = state match {
    case f: FiniteBodyState if (f.size.isEmpty && parserProvider.eosTerminatesMessage) => Some(End())
    case _ => None
  }


}

trait StreamEncoder[T <: HttpMessageHead[T]] {

  private sealed trait State
  private case object HeadState extends State
  private case class BodyState(head: T) extends State

  private var state: State = HeadState

  private def encodeChunk(data: DataBlock, buffer: DataOutBuffer) {
    buffer.write(data.size.toHexString.getBytes)
    buffer.write(HttpParse.NEWLINE)
    buffer.write(data)
    buffer.write(HttpParse.NEWLINE)
  }

  def encode(output: StreamHttpMessage[T], buffer: DataOutBuffer) {
    state match {
      case HeadState => output match {
        case Head(h) => {
          state = BodyState(h)
          h.encode(buffer)
          //need to write the final newline
          buffer write HttpParse.NEWLINE
        }
        case _ => throw new StreamHttpException("Cannot send body data before head")
      }
      case BodyState(current) => {
        output match {
          case Head(h) => throw new StreamHttpException("cannot send new head while streaming a body")
          case BodyData(data, false) if (current.headers.transferEncoding == TransferEncoding.Chunked) => {
            encodeChunk(data, buffer)
          }
          case BodyData(data, _) => buffer.write(data)
          case End() => {
            if (current.headers.transferEncoding == TransferEncoding.Chunked) {
              encodeChunk(DataBlock.Empty, buffer)
            }
            state = HeadState
          }
        }
      }
    }
  }

  def resetEncoder() {
    state = HeadState
  }


}

class StreamHttpServerCodec extends Codec[StreamHttp#ServerEncoding] with StreamDecoder[HttpRequestHead] with StreamEncoder[HttpResponseHead] {

  def parserProvider = HeadParserProvider.RequestHeadParserProvider

  def reset() {
    resetEncoder()
    resetDecoder()
  }

}
class StreamHttpClientCodec extends Codec[StreamHttp#ClientEncoding] with StreamDecoder[HttpResponseHead] with StreamEncoder[HttpRequestHead] {

  def parserProvider = HeadParserProvider.ResponseHeadParserProvider

  def reset() {
    resetEncoder()
    resetDecoder()
  }

}


abstract class StreamController[E <: Encoding {type Output = StreamHttpMessage[H]}, H <: HttpMessageHead[H], T <: HttpMessage[H]](val downstream: StreamHandler[E,H,T])
extends UpstreamEventHandler[ControllerUpstream[E]] with DownstreamEventHandler[StreamHandler[E,H,T]] with StreamHandle[E,H,T] with ControllerDownstream[E] {

  val namespace: MetricNamespace = context.worker.system.metrics
  val controllerConfig = ControllerConfig(1024, scala.concurrent.duration.Duration.Inf)

  downstream.setUpstream(this)

  def push(message: E#Output)(postWrite: QueuedItem.PostWrite = _ => ()) = {
    upstream.push(message)(postWrite)
  }

  def pushCompleteMessage(message: T)(postWrite: QueuedItem.PostWrite = _ => ()) {
    val withCL : H = message.head.withHeader(HttpHeaders.ContentLength, message.body.size.toString)
    val withCT : H = message.body.contentType.map{t => withCL.withHeader(t)}.getOrElse(withCL)
    upstream.push(Head(withCT)){_ => ()}
    upstream.push(BodyData(message.body.asDataBlock)){_ => ()}
    upstream.push(End())(postWrite)
  }

  def processMessage(m: E#Input) {
    downstream.handle(m)
  }

}

trait StreamHandle[ E <: Encoding {type Output = StreamHttpMessage[H]}, H <: HttpMessageHead[H], T <: HttpMessage[H]] extends UpstreamEvents{
  def push(message: E#Output)(postWrite: QueuedItem.PostWrite = _ => ())

  def pushCompleteMessage(message: T)(postWrite: QueuedItem.PostWrite = _ => ()) 

}

class ServerStreamController(downstream: StreamServerHandler) extends StreamController[StreamHttp#ServerEncoding, HttpResponseHead, HttpResponse](downstream) {

  val codec = new StreamHttpServerCodec

}


abstract class StreamHandler[E <: Encoding{ type Output = StreamHttpMessage[H] }, H <: HttpMessageHead[H], T <: HttpMessage[H]](val context: Context) 
extends UpstreamEventHandler[StreamHandle[E,H,T]] with HandlerTail with DownstreamEvents{

  def handle(message: E#Input)
}


abstract class StreamServerHandler(serverContext: ServerContext) 
extends StreamHandler[StreamHttp#ServerEncoding, HttpResponseHead, HttpResponse](serverContext.context) {


}


