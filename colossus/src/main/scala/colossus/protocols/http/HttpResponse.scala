package colossus
package protocols.http

import akka.util.{ByteString, ByteStringBuilder}
import colossus.controller.Source
import colossus.core.DataBuffer
import core._
import service.Callback
import controller._
import HttpParse._
import java.nio.ByteBuffer

case class HttpResponseHeader(key: ByteString, value: ByteString) {
  val keyArray = key.toArray
  val valueArray = value.toArray
}

object HttpResponseHeader {

  //TODO: these are bytestrings, whereas in Head they're strings
  val ContentLength = ByteString("content-length")
  val TransferEncoding = ByteString("transfer-encoding")

  val DELIM = ByteString(": ")
  val DELIM_ARRAY = DELIM.toArray
  val SPACE_ARRAY = Array(' '.toByte)

  def apply(key: String, value: String): HttpResponseHeader = {
    HttpResponseHeader(ByteString(key) , ByteString(value))
  }


  object Conversions {
    implicit def stringTuple2Header(t: (String, String)): HttpResponseHeader = HttpResponseHeader(t._1, t._2)
    implicit def seqStringTuple2Headers(t: Seq[(String, String)]): Array[HttpResponseHeader] = t.map{stringTuple2Header}.toArray
  }

}

case class HttpResponseHead(version : HttpVersion, code : HttpCode, headers : Array[HttpResponseHeader] = Array()) {


  def encode(buffer: DataOutBuffer) {
    buffer.write(version.messageArr)
    buffer.write(HttpResponseHeader.SPACE_ARRAY)
    buffer.write(code.headerArr)
    buffer.write(NEWLINE_ARRAY)
    var i = 0
    while (i < headers.size) {
      val header = headers(i)
      i += 1
      buffer.write(header.keyArray)
      buffer.write(HttpResponseHeader.DELIM_ARRAY)
      buffer.write(header.valueArray)
      buffer.write(NEWLINE_ARRAY)
    }
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

  //def withHeader(key: String, value: String): self.type

  def toReader: DataReader

}

//TODO: We need to make some headers like Content-Length, Transfer-Encoding,
//first-class citizens and separate them from the other headers.  This would
//prevent things like creating a response with the wrong content length

case class HttpResponse(head: HttpResponseHead, body: Option[ByteString]) extends BaseHttpResponse with Encoder {

  private def fastIntToString(in: Int, buf: DataOutBuffer) {
    if (in == 0) {
      buf.write('0'.toByte)
    } else {
      val arr = new Array[Byte](10)
      var r = in
      var index = 9
      while (r > 0) {
        arr(index) = ((r % 10) + 48).toByte
        r = r / 10
        index -= 1
      }
      buf.write(arr, index + 1, 10 - (index + 1))
    }
  }


  def encode(buffer: DataOutBuffer) {
    val dataSize = body.map{_.size}.getOrElse(0)
    head.encode(buffer)
    buffer.write(HttpResponse.ContentLengthKey.toArray)
    fastIntToString(dataSize, buffer)
    buffer.write(N2)
    body.foreach{b => 
      buffer.write(b.toArray)
    }
  }

  def resolveBody(): Option[Callback[ByteString]] = body.map{data => Callback.successful(data)}

  def withHeader(key: String, value: String) = copy(head = head.withHeader(key,value))

  def code = head.code

  def toReader = this

}

/**
  * Converter typeclass for bytestrings.  Default implementations are in package.scala
  */
trait ByteStringLike[T] {

  def toByteString(t : T) : ByteString

}

object HttpResponse {

  val ContentLengthKey = ByteString("Content-Length: ")

  def apply[T : ByteStringLike](version : HttpVersion, code : HttpCode, headers : Array[HttpResponseHeader] , data : T) : HttpResponse = {
    HttpResponse(HttpResponseHead(version, code, headers), Some(implicitly[ByteStringLike[T]].toByteString(data)))
  }


  def apply(version : HttpVersion, code : HttpCode, headers : Array[HttpResponseHeader]) : HttpResponse = {
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

  type Encoded = DataReader

  def toReader : DataReader = {
    val builder = new DynamicOutBuffer(100, false)
    head.encode(builder)
    builder write NEWLINE_ARRAY

    val headerBytes = builder.data
    body.map{stream => 
      DataStream(new DualSource[DataBuffer](Source.one(headerBytes), stream))
    }.getOrElse(headerBytes)

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

  def apply[T : ByteStringLike](version : HttpVersion, code : HttpCode, headers : Array[HttpResponseHeader] = Array(), data : T) : StreamingHttpResponse = {
    fromStatic(HttpResponse(
      HttpResponseHead(version, code, headers), 
      Some(implicitly[ByteStringLike[T]].toByteString(data))
    ))
  }

}




