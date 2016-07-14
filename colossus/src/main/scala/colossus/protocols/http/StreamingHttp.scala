package colossus
package protocols.http
package stream

import colossus.metrics.MetricNamespace
import controller._
import service.Protocol
import core._


trait HttpMessageType {
  type HeadType <: HttpMessageHead
}

trait HttpRequestType extends HttpMessageType {
  type HeadType = HttpRequestHead
}
trait HttpResponseType extends HttpMessageType {
  type HeadType = HttpResponseHead
}

sealed trait StreamHttpRequest
sealed trait StreamHttpResponse

case class RequestHead(head: HttpRequestHead) extends StreamHttpRequest
case class ResponseHead(head: HttpResponseHead) extends StreamHttpResponse

sealed trait StreamBodyMessage extends StreamHttpRequest with StreamHttpResponse
/**
 * A Piece of data for a http message body.  `chunkEncoded` declares whether the data
 * is raw data(false) or a properly encoded http chunk (true).  In most cases
 * this should be false unless the data is being proxied verbatim.
 */
case class BodyData(data: DataBlock, chunkEncoded: Boolean = false) extends StreamBodyMessage
case object End extends StreamBodyMessage


trait StreamHttp extends Protocol {


  type Request = StreamHttpRequest
  type Response = StreamHttpResponse
}

//TODO - generalize this for both servers and clients
trait StreamDecoder extends Codec[StreamHttp#ServerEncoding]{ 


  private var headParser = HttpRequestParser.httpHead

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

  def decode(data: DataBuffer): Option[StreamHttpRequest] = state match {
    case HeadState => headParser.parse(data) match {
      case Some(h) => {
        if (h.headers.transferEncoding == TransferEncoding.Chunked) {
          state = new ChunkedBodyState
        } else {
          state = new FiniteBodyState(h.headers.contentLength)
        }
        Some(RequestHead(h))
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


  override def reset() {
    state = HeadState
    headParser = HttpRequestParser.httpHead
  }


}

trait StreamEncoder extends Codec[StreamHttp#ServerEncoding]{

  sealed trait State
  case object Head extends State
  case class Body(head: HttpResponseHead) extends State

  private var state: State = Head

  private def encodeChunk(data: DataBlock, buffer: DataOutBuffer) {
    buffer.write(data.size.toHexString.getBytes)
    buffer.write(HttpParse.NEWLINE)
    buffer.write(data)
    buffer.write(HttpParse.NEWLINE)
  }

  def encode(output: StreamHttpResponse, buffer: DataOutBuffer) {
    state match {
      case Head => output match {
        case ResponseHead(h) => {
          state = Body(h)
          h.encode(buffer)
          //need to write the final newline
          buffer write HttpParse.NEWLINE
        }
        case _ => throw new Exception("Cannot send body data before head")
      }
      case Body(current) => {
        output match {
          case ResponseHead(h) => throw new Exception("cannot send new head while streaming a response")
          case BodyData(data, false) if (current.headers.transferEncoding == TransferEncoding.Chunked) => {
            encodeChunk(data, buffer)
          }
          case BodyData(data, _) => buffer.write(data)
          case End => {
            if (current.headers.transferEncoding == TransferEncoding.Chunked) {
              encodeChunk(DataBlock.Empty, buffer)
            }
            state = Head
          }
        }
      }
    }
  }

  override def reset() {
    state = Head
  }

  def endOfStream() = None

}

class StreamHttpServerCodec extends StreamDecoder with StreamEncoder {


}

abstract class StreamServerHandler(context: ServerContext) 
extends BasicController[StreamHttp#ServerEncoding](new StreamHttpServerCodec, ControllerConfig(1024, scala.concurrent.duration.Duration.Inf), context.context) 
with ServerConnectionHandler with ControllerIface[StreamHttp#ServerEncoding]{

  val namespace: MetricNamespace = context.server.system.metrics

  def receivedMessage(message: Any,sender: akka.actor.ActorRef): Unit = ???


  protected def pushResponse(response: HttpResponse)(postWrite: QueuedItem.PostWrite) {
    val withCL = response.head.withHeader(HttpHeaders.ContentLength, response.body.size.toString)
    val withCT = response.body.contentType.map{t => withCL.withHeader(t)}.getOrElse(withCL)
    push(ResponseHead(withCT)){_ => ()}
    push(BodyData(response.body.asDataBlock)){_ => ()}
    push(End)(postWrite)
  }

}


