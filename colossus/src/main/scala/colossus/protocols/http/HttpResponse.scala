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

  def apply(key: String, value: String): HttpResponseHeader = {
    HttpResponseHeader(ByteString(key) , ByteString(value))
  }


  object Conversions {
    implicit def stringTuple2Header(t: (String, String)): HttpResponseHeader = HttpResponseHeader(t._1, t._2)
    implicit def seqStringTuple2Headers(t: Seq[(String, String)]): Vector[HttpResponseHeader] = t.map{stringTuple2Header}.toVector
  }

}

case class HttpResponseHead(version : HttpVersion, code : HttpCode, headers : Vector[HttpResponseHeader] = Vector()) {

  def appendHeaderBytes(builder : ByteStringBuilder) {
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
  }

  def doit(buffer: ByteBuffer) {
    buffer.put(version.messageArr)
    buffer.put(' '.toByte)
    buffer.put(code.headerArr)
    buffer.put(NEWLINE_ARRAY)
    var i = 0
    while (i < headers.size) {
      val header = headers(i)
      i += 1
      buffer.put(header.keyArray)
      buffer.put(HttpResponseHeader.DELIM_ARRAY)
      buffer.put(header.valueArray)
      buffer.put(NEWLINE_ARRAY)
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
    //val builder = new ByteStringBuilder
    val buffer = ByteBuffer.allocate(200)
    val dataSize = body.map{_.size}.getOrElse(0)
    //builder.sizeHint((50 * head.headers.size) + dataSize)
    //head.appendHeaderBytes(builder)
    head.doit(buffer)
    buffer.put(HttpResponse.ContentLengthKey.toArray)
    buffer.put(dataSize.toString.getBytes)
    buffer.put(N2)
    //builder append HttpResponse.ContentLengthKey
    //builder putBytes dataSize.toString.getBytes
    //builder append NEWLINE
    //builder append NEWLINE
    body.foreach{b => 
      //builder append b
      buffer.put(b.toArray)
    }
    //DataBuffer(builder.result())
    buffer.flip
    DataBuffer(buffer)
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

  def apply[T : ByteStringLike](version : HttpVersion, code : HttpCode, headers : Vector[HttpResponseHeader] = Vector(), data : T) : StreamingHttpResponse = {
    fromStatic(HttpResponse(
      HttpResponseHead(version, code, headers), 
      Some(implicitly[ByteStringLike[T]].toByteString(data))
    ))
  }

}




