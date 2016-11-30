package colossus
package protocols.http

import core._

import akka.util.{ByteString, ByteStringBuilder}

import colossus.parsing._
import HttpParse._
import Combinators._
import controller._
import java.nio.ByteBuffer
import service.DecodedResult
import scala.language.higherKinds

sealed trait ResponseResult
object ResponseResult {
  case class StaticResponse(response: HttpResponse) extends ResponseResult
  case class StreamResponse(sink: Sink[DataBuffer], response: HttpResponse) extends ResponseResult
}

object HttpResponseParser  {

  def static(): Parser[DecodedResult.Static[HttpResponse]] = staticBody(true)

  def stream(dechunkBody: Boolean): Parser[DecodedResult[StreamingHttpResponse]] = streamBody(dechunkBody)

  import HttpBody._


  //TODO: eliminate duplicate code
  //TODO: Dechunk on static

  protected def staticBody(dechunk: Boolean): Parser[DecodedResult.Static[HttpResponse]] = head |> {parsedHead =>
    parsedHead.headers.transferEncoding match {
      case TransferEncoding.Identity => parsedHead.headers.contentLength match {
        case Some(0)  => const(DecodedResult.Static(HttpResponse(parsedHead, HttpBody.NoBody)))
        case Some(n)  => bytes(n) >> {body => DecodedResult.Static(HttpResponse(parsedHead, new HttpBody(body)))}
        case None if (parsedHead.code.isInstanceOf[NoBodyCode]) => const(DecodedResult.Static(HttpResponse(parsedHead, HttpBody.NoBody)))
        case None     => bytesUntilEOS >> {body => DecodedResult.Static(HttpResponse(parsedHead, body))}
      }
      case _  => chunkedBody >> {body => DecodedResult.Static(HttpResponse(parsedHead, body))}
    }
  }

  protected def streamBody(dechunk: Boolean): Parser[DecodedResult[StreamingHttpResponse]] = head >> {parsedHead =>
    parsedHead.headers.transferEncoding match {
      case TransferEncoding.Identity => parsedHead.headers.contentLength match {
        case Some(0)=> DecodedResult.Static(StreamingHttpResponse(parsedHead, None))
        case Some(n) => streamingResponse(parsedHead, Some(n), false)
        case None if (parsedHead.code.isInstanceOf[NoBodyCode]) => DecodedResult.Static(StreamingHttpResponse(parsedHead, None))
        //TODO: adding support for this requires upcoming changes to stream termination error handling
        case None => throw new ParseException("Infinite non-chunked responses not supported")
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


  protected def head: Parser[HttpResponseHead] = firstLine ~ headers >> {case fl ~ hbuilder =>
    HttpResponseHead(fl, hbuilder.buildHeaders)
  }

  protected def firstLine = line(true) >> ParsedResponseFL.apply


}

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


  private val chunkParser: Parser[Array[Byte]] = intUntil('\r', 16) <~ byte |> {
    case 0 => const(Array())
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
        case Some(fullChunk)  => if (fullChunk.length > 0) {
          super.push(DataBuffer(ByteBuffer.wrap(fullChunk)))
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

