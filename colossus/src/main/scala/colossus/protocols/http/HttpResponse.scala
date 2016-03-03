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


object HttpResponseHeader {

  //TODO: these are bytestrings, whereas in Head they're strings
  val ContentLength = ByteString("content-length")
  val TransferEncoding = ByteString("transfer-encoding")

  val DELIM = ByteString(": ")
  val DELIM_ARRAY = DELIM.toArray
  val SPACE_ARRAY = Array(' '.toByte)


}

case class HttpResponseHead(version : HttpVersion, code : HttpCode, headers : HttpHeaders ) {


  def encode(buffer: DataOutBuffer) {
    buffer.write(version.messageArr)
    buffer.write(HttpResponseHeader.SPACE_ARRAY)
    buffer.write(code.headerArr)
    buffer.write(NEWLINE_ARRAY)
    headers.encode(buffer)
  }

  def withHeader(key: String, value: String) = copy(headers = headers + (key -> value))

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


class HttpResponseBody(private val body: Array[Byte]) extends AnyVal {

  def size = body.size

  def encode(buffer: DataOutBuffer) {
    if (size > 0) buffer.write(body)
  }

  def bytes: ByteString = ByteString(body)

}

object HttpResponseBody {
  
  val NoBody = HttpResponseBody("")
  
  def apply(data: ByteString): HttpResponseBody = new HttpResponseBody(data.toArray)
  def apply(data: String) : HttpResponseBody = new HttpResponseBody(data.getBytes("UTF-8"))

}

case class HttpResponse(head: HttpResponseHead, body: HttpResponseBody) extends BaseHttpResponse with Encoder {

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
    head.encode(buffer)
    buffer.write(HttpResponse.ContentLengthKey.toArray)
    fastIntToString(body.size, buffer)
    buffer.write(N2)
    body.encode(buffer)
  }

  def resolveBody(): Option[Callback[ByteString]] = if (body.size > 0) {
    Some(Callback.successful(body.bytes))
  } else None

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

  def apply[T : ByteStringLike](head: HttpResponseHead, body: T): HttpResponse = {
    HttpResponse(head, HttpResponseBody(implicitly[ByteStringLike[T]].toByteString(body)))
  }

  def apply[T : ByteStringLike](version : HttpVersion, code : HttpCode, headers : HttpHeaders , data : T) : HttpResponse = {
    HttpResponse(HttpResponseHead(version, code, headers), data)
  }


  def apply(version : HttpVersion, code : HttpCode, headers : HttpHeaders) : HttpResponse = {
    HttpResponse(HttpResponseHead(version, code, headers), new HttpResponseBody(Array()))
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
    StreamingHttpResponse(resp.head.withHeader(HttpHeaders.ContentLength, resp.body.size.toString), Some(Source.one(DataBuffer(resp.body.bytes))))
  }

  def apply[T : ByteStringLike](version : HttpVersion, code : HttpCode, headers : HttpHeaders, data : T) : StreamingHttpResponse = {
    fromStatic(HttpResponse(
      HttpResponseHead(version, code, headers), 
      HttpResponseBody(implicitly[ByteStringLike[T]].toByteString(data))
    ))
  }

}




