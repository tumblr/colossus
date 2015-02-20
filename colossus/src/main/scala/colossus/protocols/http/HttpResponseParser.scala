package colossus
package protocols.http

import core._

import akka.util.{ByteString, ByteStringBuilder}

import colossus.parsing._
import HttpParse._
import Combinators._
import DataSize._
import controller._
import service.DecodedResult
import scala.language.higherKinds

sealed trait ResponseResult 
object ResponseResult {
  case class StaticResponse(response: HttpResponse) extends ResponseResult
  case class StreamResponse(sink: Sink[DataBuffer], response: HttpResponse) extends ResponseResult
}

object HttpResponseParser  {
  val DefaultMaxSize: DataSize = 10.MB

  //TODO: eliminate duplicate code
  //TODO: Dechunk on static

  def staticBody(dechunk: Boolean): Parser[DecodedResult.Static[HttpResponse]] = head |> {parsedHead =>
    parsedHead.transferEncoding match {
      case TransferEncoding.Identity => parsedHead.contentLength match {
        case Some(0) | None => const(DecodedResult.Static(HttpResponse(parsedHead, None)))
        case Some(n) => bytes(n) >> {body => DecodedResult.Static(HttpResponse(parsedHead, Some(body)))}
      }
      case _  => chunkedBody >> {body => DecodedResult.Static(HttpResponse(parsedHead, Some(body)))}
    }
  }

  def streamBody(dechunk: Boolean): Parser[DecodedResult[StreamingHttpResponse]] = head >> {parsedHead =>
    parsedHead.transferEncoding match {
      case TransferEncoding.Identity => parsedHead.contentLength match {
        case Some(0) | None => DecodedResult.Static(StreamingHttpResponse(parsedHead, None))
        case Some(n) => streamingResponse(parsedHead, Some(n), false)
      }
      case _  => streamingResponse(parsedHead, None, dechunk)
    }
  }

  private def streamingResponse(head: HttpResponseHead, contentLength: Option[Int], dechunk: Boolean) = {
    val pipe: Pipe[DataBuffer, DataBuffer] = contentLength match {
      case Some(n)  => new FiniteBytePipe(n)
      case None     => if (dechunk) new ChunkDecodingPipe else new ChunkPassThroughPipe
    }
    DecodedResult.Stream(StreamingHttpResponse(head, Some(pipe)), pipe)
  }
    

  protected def head: Parser[HttpResponseHead] = firstLine ~ headers >> {case version ~ code ~ headers => HttpResponseHead(version, code, headers)}

  protected def firstLine = version ~ code 

  protected def version = stringUntil(' ') >> {v => HttpVersion(v)}

  protected def code = intUntil(' ') <~ stringUntil('\r') <~ byte >> {c => HttpCode(c.toInt)}

}

/** The base class used for both the static and streaming response parsers
 * @tparam D This simply lets the static version have its return type DecodedResult.Static
 * @tparam T The type of HttpResponse the parser will return
 */
class BaseHttpResponseParser[D[_] <: DecodedResult[_], T <: BaseHttpResponse](parserFactory:() => Parser[D[T]]) {

  private var current : Parser[D[T]] = parserFactory()

  def parse(data : DataBuffer) : Option[D[T]] = current.parse(data)

  def reset() {
    current = parserFactory()
  }
}

class HttpResponseParser(maxResponseSize: DataSize = HttpResponseParser.DefaultMaxSize) 
  extends BaseHttpResponseParser[DecodedResult.Static, HttpResponse](() => maxSize(maxResponseSize, HttpResponseParser.staticBody(true)))


/**
 * @param dechunkBody if a response is sent using chunked transfer-encoding,
 * the chunk headers will be removed from the data before being pushed into the
 * pipe.  This should genrally be set to true if you are directly processing
 * the incoming data, and should be false if you are going to be proxying the
 * response somewhere else.
 */
class StreamingHttpResponseParser(dechunkBody: Boolean) 
  extends BaseHttpResponseParser[DecodedResult, StreamingHttpResponse](() => HttpResponseParser.streamBody(dechunkBody))

object HttpChunk {
  
  def wrap(data: DataBuffer): DataBuffer = {
    val builder = new ByteStringBuilder
    builder.sizeHint(data.size + 25)
    builder.putBytes(data.size.toHexString.getBytes)
    builder.append(HttpParse.NEWLINE)
    builder.putBytes(data.takeAll)
    builder.append(HttpParse.NEWLINE)
    DataBuffer(builder.result)
  }
}

/** A Pipe that will take raw DataBuffers and add a http chunk header.  This is
 * needed if you are streaming out a response that is not also being streamed
 * in (like in a proxy)
 *
 * Right now this ends up copying the DataBuffer.
 * 
 */
class ChunkEncodingPipe extends InfinitePipe[DataBuffer] {

  override def push(data: DataBuffer) = whenPushable {
    super.push(HttpChunk.wrap(data))
  }

  override def complete() {
    super.push(DataBuffer(ByteString("0\r\n\r\n"))) match {
      case PushResult.Full(trig) => trig.fill(complete)
      case _ => super.complete()
    }
  }

}


/**
 * A Pass-through pipe that parses chunk headers in the data stream and closes
 * the pipe when the stream ends.  This pipe does not remove any header/footer
 * information, making it ideal for use in a proxy
 */
class ChunkPassThroughPipe extends InfinitePipe[DataBuffer] {

  //this parser is used to follow along with the chunks so we know when to
  //close the stream, but we don't actually parse out the data since we're just
  //passing everything through
  private val chunkParser = intUntil('\r', 16) <~ byte |>{
    case 0   => const(0) //notice there's no second \r\n, this is according to spec
    case len => skip(len.toInt) <~ bytes(2) >> {_ => len.toInt}
  }

  private val trailerParser = bytes(2)

  private var parsingTrailer = false

  override def push(data: DataBuffer): PushResult = whenPushable {
    val (done, bytesRead) = data.peek{d => 
      try {
        if (!parsingTrailer) {
          var chunkSize = Int.MaxValue
          while(d.hasUnreadData && chunkSize != 0) {
            chunkParser.parse(d).foreach{len =>
              chunkSize = len
            }
          }
          parsingTrailer = if (chunkSize == 0) true else false
        }
        if (parsingTrailer) {
          if (trailerParser.parse(d).isDefined) {
            true
          } else {
            false
          }
        } else {
          false
        }        
      } catch {
        case p: ParseException => {
          terminate(p)
          false //this will result in a push failure
        }
      }
    }
    if (done) {
      super.push(DataBuffer(data.take(bytesRead)))
      complete()
      PushResult.Complete
    } else {
      super.push(data)
    }
  }

}

/** A pipe designed to accept a chunked http body.
 *
 * The pipe will scan the chunk headers and auto-close itself when it hits the
 * end of the stream (reading a 0 length chunk).  It also strips all chunk
 * headers from the data, so the output stream is the raw data
 *  
 */
class ChunkDecodingPipe extends InfinitePipe[DataBuffer] {

  
  private val chunkParser = intUntil('\r', 16) <~ byte |> {
    case 0 => const(ByteString())
    case n => bytes(n.toInt) <~ bytes(2)
  }

  private val trailerParser = bytes(2)

  private var parsingTrailer = false

  override def push(data: DataBuffer): PushResult = whenPushable {
    if (parsingTrailer) {
      if (trailerParser.parse(data).isDefined) {
        complete()
        PushResult.Complete
      } else {
        PushResult.Ok
      }
    } else {
      chunkParser.parse(data) match {
        case Some(fullChunk)  => if (fullChunk.size > 0) {
          super.push(DataBuffer(fullChunk))
          push(data)
        } else {
          //terminal chunk
          parsingTrailer = true
          push(data)
        }
        case None => PushResult.Ok
      }
    }
  }

}
    
