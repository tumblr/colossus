package colossus
package protocols.http

import akka.util.{ByteString, ByteStringBuilder}
import colossus.controller.Source
import colossus.core.DataBuffer
import core._
import service.Callback
import controller._
import HttpParse._
import encoding._


case class HttpResponseHeader(key: ByteString, value: ByteString)

object HttpResponseHeader {

  //TODO: these are bytestrings, whereas in Head they're strings
  val ContentLength = ByteString("content-length")
  val TransferEncoding = ByteString("transfer-encoding")

  val DELIM = ByteString(": ")

  def apply(key: String, value: String): HttpResponseHeader = {
    HttpResponseHeader(ByteString(key) , ByteString(value))
  }


  object Conversions {
    implicit def stringTuple2Header(t: (String, String)): HttpResponseHeader = HttpResponseHeader(t._1, t._2)
    implicit def seqStringTuple2Headers(t: Seq[(String, String)]): Vector[HttpResponseHeader] = t.map{stringTuple2Header}.toVector
  }

}

case class HttpResponseHead(version : HttpVersion, code : HttpCode, headers : Vector[HttpResponseHeader] = Vector()) {

  def encode: Encoder =  {
    val builder = new ByteStringBuilder()
    builder.sizeHint(50 * headers.size)
    builder append version.messageBytes
    builder putByte ' '
    builder append code.headerBytes
    builder append NEWLINE
    headers.foreach{case header =>
      builder ++= header.key
      builder ++= HttpResponseHeader.DELIM
      builder ++= header.value
      builder ++= NEWLINE
    }
    builder append NEWLINE
    Encoders.block(builder.result)
  }

  def withHeader(key: String, value: String) = copy(headers = headers :+ HttpResponseHeader(key, value))

  def contentLength = {
    headers.collectFirst{case HttpResponseHeader(HttpResponseHeader.ContentLength, v) => v.utf8String.toInt}
  }

  def transferEncoding : TransferEncoding = {
    headers.collectFirst{case HttpResponseHeader(HttpResponseHeader.TransferEncoding, v) => v}.flatMap{t => TransferEncoding.unapply(t.utf8String)}.getOrElse(TransferEncoding.Identity)
}

}

sealed trait BaseHttpResponse { 

  def head: HttpResponseHead

  /**
   * Resolves the body into a bytestring.  This operation completes immediately
   * for static responses.  For streaming responses, this only completes when
   * all data has been received.  Be aware if the response is an infinite
   * stream of data (using chunked transfer encoding), this will never
   * complete.
   */
  def resolveBody(): Option[Callback[ByteString]]

  def encode(): Encoder

  //def withHeader(key: String, value: String): self.type

}

sealed trait HttpResponseBody {
  def encode: Encoder
}
object HttpResponseBody {
  case object NoBody extends HttpResponseBody {
    def encode = Encoders.Zero
  }
  case class Data(data: ByteString) extends HttpResponseBody {
    def encode = Encoders.block(data)
  }
  case class Stream(data: Source[DataBuffer]) extends HttpResponseBody
}

//TODO: We need to make some headers like Content-Length, Transfer-Encoding,
//first-class citizens and separate them from the other headers.  This would
//prevent things like creating a response with the wrong content length

case class HttpResponse(head: HttpResponseHead, body: HttpResponseBody) extends BaseHttpResponse {

  type Encoded = DataBuffer

  def encode() : Encoder = Encoders.unsized {
    val builder = new ByteStringBuilder
    val dataSize = body.map{_.size}.getOrElse(0)
    builder.sizeHint((50 * head.headers.size) + dataSize)
    head.appendHeaderBytes(builder)
    builder append HttpResponse.ContentLengthKey
    builder putBytes dataSize.toString.getBytes
    body.foreach{b => 
      builder append b
    }
    DataBuffer(builder.result())
  }

  def resolveBody(): Option[Callback[ByteString]] = body.map{data => Callback.successful(data)}

  def withHeader(key: String, value: String) = copy(head = head.withHeader(key,value))

  def code = head.code

}

/**
  * Converter typeclass for bytestrings.  Default implementations are in package.scala
  */
trait ByteStringLike[T] {

  def toByteString(t : T) : ByteString

}

object HttpResponse {

  val ContentLengthKey = ByteString("Content-Length: ")

  def apply[T : ByteStringLike](version : HttpVersion, code : HttpCode, headers : Vector[HttpResponseHeader] , data : T) : HttpResponse = {
    HttpResponse(HttpResponseHead(version, code, headers), Some(implicitly[ByteStringLike[T]].toByteString(data)))
  }


  def apply(version : HttpVersion, code : HttpCode, headers : Vector[HttpResponseHeader]) : HttpResponse = {
    HttpResponse(HttpResponseHead(version, code, headers), None)
  }

}


/**
 * Be aware, at the moment when the response is encoded, there is no processing
 * on the response body, and no headers are added in.  This means if the
 * transfer-encoding is "chunked", the header must already exist and the stream
 * must already be prepending chunk headers to the data.
 */
case class StreamingHttpResponse(head: HttpResponseHead, body: Option[Source[DataBuffer]]) extends BaseHttpResponse {


  def encode() : Encoder = Encoders.unsized {
    val builder = new ByteStringBuilder
    builder.sizeHint(100)
    head.appendHeaderBytes(builder)
    builder append NEWLINE
    //TODO: FIXX!!!!!!!
    DataBuffer(builder.result)
    /*
    val headerBytes = DataBuffer(builder.result())
    body.map{stream => 
      DataStream(new DualSource[DataBuffer](Source.one(headerBytes), stream))
    }.getOrElse(headerBytes)
    */

  }

  def resolveBody: Option[Callback[ByteString]] = body.map{data => 
    data.fold(new ByteStringBuilder){(buffer: DataBuffer, builder: ByteStringBuilder) => builder.putBytes(buffer.takeAll)}.map{_.result}
  }

  def withHeader(key: String, value: String) = copy(head = head.withHeader(key,value))

}

object StreamingHttpResponse {

  def fromStatic(resp: HttpResponse): StreamingHttpResponse = {
    StreamingHttpResponse(resp.head.withHeader(HttpHeaders.ContentLength, resp.body.map{_.size}.getOrElse(0).toString), resp.body.map{b => Source.one(DataBuffer(b))})
  }

  def apply[T : ByteStringLike](version : HttpVersion, code : HttpCode, headers : Vector[HttpResponseHeader] = Vector(), data : T) : StreamingHttpResponse = {
    fromStatic(HttpResponse(
      HttpResponseHead(version, code, headers), 
      Some(implicitly[ByteStringLike[T]].toByteString(data))
    ))
  }

}




