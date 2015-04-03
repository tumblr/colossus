package colossus
package protocols.http

import akka.util.{ByteString, ByteStringBuilder}
import colossus.controller.Source
import colossus.core.DataBuffer
import core._
import service.Callback
import controller._
import HttpParse._


case class HttpResponseHead(version : HttpVersion, code : HttpCode, headers : Vector[(String, String)] = Vector()) extends HttpHeaderUtils {

  def appendHeaderBytes(builder : ByteStringBuilder) {
    builder append version.messageBytes
    builder putByte ' '
    builder append code.headerBytes
    builder append NEWLINE
    headers.foreach{case (key, value) =>
      builder putBytes key.getBytes
      builder putBytes ": ".getBytes
      builder putBytes value.getBytes
      builder append NEWLINE
    }
  }

  def withHeader(key: String, value: String) = copy(headers = headers :+ (key, value))

}

sealed trait BaseHttpResponse { 

  type Encoded <: DataReader

  def head: HttpResponseHead

  /**
   * Resolves the body into a bytestring.  This operation completes immediately
   * for static responses.  For streaming responses, this only completes when
   * all data has been received.  Be aware if the response is an infinite
   * stream of data (using chunked transfer encoding), this will never
   * complete.
   */
  def resolveBody(): Option[Callback[ByteString]]

  def encode(): Encoded

  //def withHeader(key: String, value: String): self.type

}

//TODO: We need to make some headers like Content-Length, Transfer-Encoding,
//first-class citizens and separate them from the other headers.  This would
//prevent things like creating a response with the wrong content length

case class HttpResponse(head: HttpResponseHead, body: Option[ByteString]) extends BaseHttpResponse {

  type Encoded = DataBuffer

  def encode() : DataBuffer = {
    val builder = new ByteStringBuilder
    val dataSize = body.map{_.size}.getOrElse(0)
    builder.sizeHint(100 + dataSize)
    head.appendHeaderBytes(builder)
    builder putBytes s"Content-Length: ${dataSize}".getBytes
    builder append NEWLINE
    builder append NEWLINE
    body.foreach{b => 
      builder append b
    }
    DataBuffer(builder.result())
  }

  def resolveBody(): Option[Callback[ByteString]] = body.map{data => Callback.successful(data)}

  def withHeader(key: String, value: String) = copy(head = head.withHeader(key,value))

  val code = head.code

}

/**
  * Converter typeclass for bytestrings.  Default implementations are in package.scala
  */
trait ByteStringLike[T] {

  def toByteString(t : T) : ByteString

}

object HttpResponse {

  def apply[T : ByteStringLike](version : HttpVersion, code : HttpCode, headers : Vector[(String, String)] , data : T) : HttpResponse = {
    HttpResponse(HttpResponseHead(version, code, headers), Some(implicitly[ByteStringLike[T]].toByteString(data)))
  }


  def apply(version : HttpVersion, code : HttpCode, headers : Vector[(String, String)]) : HttpResponse = {
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

  def encode() : DataReader = {
    val builder = new ByteStringBuilder
    builder.sizeHint(100)
    head.appendHeaderBytes(builder)
    builder append NEWLINE

    val headerBytes = DataBuffer(builder.result())
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

  def apply[T : ByteStringLike](version : HttpVersion, code : HttpCode, headers : Vector[(String, String)] = Vector(), data : T) : StreamingHttpResponse = {
    fromStatic(HttpResponse(
      HttpResponseHead(version, code, headers), 
      Some(implicitly[ByteStringLike[T]].toByteString(data))
    ))
  }

}




