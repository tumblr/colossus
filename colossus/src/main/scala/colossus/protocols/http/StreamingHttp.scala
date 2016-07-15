package colossus
package protocols.http
package stream

import colossus.metrics.MetricNamespace
import controller._
import service.Protocol
import core._
import parsing.Combinators.Parser


trait HttpMessageType {
  type HeadType <: HttpMessageHead
}

trait HttpRequestType extends HttpMessageType {
  type HeadType = HttpRequestHead
}
trait HttpResponseType extends HttpMessageType {
  type HeadType = HttpResponseHead
}

sealed trait StreamHttpMessage[+T <: HttpMessageHead]

case class Head[+T <: HttpMessageHead](head: T) extends StreamHttpMessage[T]

sealed trait StreamBodyMessage extends StreamHttpMessage[Nothing]
/**
 * A Piece of data for a http message body.  `chunkEncoded` declares whether the data
 * is raw data(false) or a properly encoded http chunk (true).  In most cases
 * this should be false unless the data is being proxied verbatim.
 */
case class BodyData(data: DataBlock, chunkEncoded: Boolean = false) extends StreamBodyMessage
case object End extends StreamBodyMessage


trait StreamHttp extends Protocol {


  type Request = StreamHttpMessage[HttpRequestHead]
  type Response = StreamHttpMessage[HttpResponseHead]
}

trait HeadParserProvider[T <: HttpMessageHead] {
  def parser: Parser[T]
  def eosTerminatesMessage : Boolean
}
object HeadParserProvider {
  implicit object RequestHeadParserProvider extends HeadParserProvider[HttpRequestHead] {
    def parser = HttpRequestParser.httpHead

    def eosTerminatesMessage = false

    
  }
}

//TODO - generalize this for both servers and clients
trait StreamDecoder[T <: HttpMessageHead] { 

  def parserProvider: HeadParserProvider[T]

  private var headParser = parserProvider.parser

  sealed trait State
  case object HeadState extends State
  sealed trait BodyState extends State {
    def nextPiece(input: DataBuffer): Option[StreamBodyMessage]
  }
  //used when the transfer-encoding is identity or a content-length is provided
  //(so non-chunked encoding)
  class FiniteBodyState(val size: Option[Int]) extends BodyState {
    private var taken = 0
    def done = size.map{_ == taken}.getOrElse(false)
    def nextPiece(input: DataBuffer): Option[StreamBodyMessage] = if (done) {
      Some(End)
    } else if (input.hasUnreadData) {
      val bytes = input.take(size.map{_ - taken}.getOrElse(input.remaining))
      taken += bytes.length
      Some(BodyData(DataBlock(bytes), false))
    } else {
      None
    }
  }
  class ChunkedBodyState extends BodyState {
    import parsing.Combinators._
    val parser: Parser[StreamBodyMessage] = intUntil('\r', 16) <~ byte |> {
      case 0 => bytes(2) >> {_ => End}
      case n => bytes(n.toInt) <~ bytes(2) >> {bytes => BodyData(DataBlock(bytes), false)}
    }
    def nextPiece(input: DataBuffer): Option[StreamBodyMessage] = parser.parse(input)
  
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
      case Some(End) => {
        state = HeadState
        Some(End)
      }
      case other => other
    }
  }


  def resetDecoder() {
    state = HeadState
    headParser = parserProvider.parser
  }


}

trait StreamEncoder[T <: HttpMessageHead] {

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
        case _ => throw new Exception("Cannot send body data before head")
      }
      case BodyState(current) => {
        output match {
          case Head(h) => throw new Exception("cannot send new head while streaming a body")
          case BodyData(data, false) if (current.headers.transferEncoding == TransferEncoding.Chunked) => {
            encodeChunk(data, buffer)
          }
          case BodyData(data, _) => buffer.write(data)
          case End => {
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

  def endOfStream() = None

}

class StreamHttpServerCodec extends Codec[StreamHttp#ServerEncoding] with StreamDecoder[HttpRequestHead] with StreamEncoder[HttpResponseHead] {

  def parserProvider = HeadParserProvider.RequestHeadParserProvider

  def reset() {
    resetEncoder()
    resetDecoder()
  }

}

abstract class StreamServerHandler(context: ServerContext) 
extends BasicController[StreamHttp#ServerEncoding](new StreamHttpServerCodec, ControllerConfig(1024, scala.concurrent.duration.Duration.Inf), context.context) 
with ServerConnectionHandler with ControllerIface[StreamHttp#ServerEncoding]{

  val namespace: MetricNamespace = context.server.system.metrics

  def receivedMessage(message: Any,sender: akka.actor.ActorRef): Unit = ???

  protected def pushResponse(response: HttpResponse)(postWrite: QueuedItem.PostWrite) {
    val withCL = response.head.withHeader(HttpHeaders.ContentLength, response.body.size.toString)
    val withCT = response.body.contentType.map{t => withCL.withHeader(t)}.getOrElse(withCL)
    push(Head(withCT)){_ => ()}
    push(BodyData(response.body.asDataBlock)){_ => ()}
    push(End)(postWrite)
  }

}


