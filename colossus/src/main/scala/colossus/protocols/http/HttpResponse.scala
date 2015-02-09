package colossus
package protocols.http

import akka.util.ByteString
import colossus.controller.{FiniteBytePipe, Source}
import colossus.core.DataBuffer

trait HttpResponseHeader extends HttpHeaderUtils{
  def version : HttpVersion
  def code : HttpCode
  def headers : Seq[(String, String)]

}

case class HttpResponse(version : HttpVersion, code : HttpCode, headers : Seq[(String, String)] = Nil, data : ByteString) extends HttpResponseHeader {

  def withHeader(key: String, value: String) = copy(headers = headers :+ (key, value))

  override def equals(other: Any) = other match {
    case HttpResponse(v, c, h, b) => v == version && c == code && b == data && h.toSet == headers.toSet
    case _ => false
  }
}

object HttpResponse {

  def fromValue[T : ByteStringLike](version : HttpVersion, code : HttpCode, headers : List[(String, String)] = Nil, data : T) : HttpResponse = {
    HttpResponse(version, code, headers, implicitly[ByteStringLike[T]].toByteString(data))
  }
}

case class StreamingHttpResponse(version : HttpVersion, code : HttpCode, headers : Seq[(String, String)] = Nil, stream : Source[DataBuffer]) extends HttpResponseHeader {
  def withHeader(key: String, value: String) = copy(headers = headers :+ (key, value))
  

  override def equals(other: Any) = other match {
    case StreamingHttpResponse(v, c, h, s) => v == version && c == code && h.toSet == headers.toSet
    case _ => false
  }
}

object StreamingHttpResponse {

  def fromStatic(response : HttpResponse) : StreamingHttpResponse = {
    val buffer = response.data.asByteBuffer
    val data = new DataBuffer(buffer)
    val pipe = new FiniteBytePipe(response.data.size, HttpResponseParser.DefaultQueueSize)
    pipe.push(data)
    val strippedHeaders = response.headers.filterNot{_._1 == HttpHeaders.ContentLength}
    val finalHeaders = strippedHeaders :+ (HttpHeaders.ContentLength, response.data.size.toString)
    StreamingHttpResponse(response.version, response.code, finalHeaders, pipe)
  }

  def fromValue[T : ByteStringLike](version : HttpVersion = HttpVersion.`1.1`, code : HttpCode, headers :Seq[(String, String)] = Nil, value : T) : StreamingHttpResponse = {
    val byteString = implicitly[ByteStringLike[T]].toByteString(value)
    val strippedHeaders = headers.filterNot{_._1 == HttpHeaders.ContentLength}
    val finalHeaders = strippedHeaders :+ (HttpHeaders.ContentLength, byteString.size.toString)
    StreamingHttpResponse(version, code, finalHeaders, Source.one(DataBuffer(byteString)))
  }
}

trait ByteStringLike[T] {

  def toByteString(t : T) : ByteString

}

trait ByteStringConverters {

  implicit object ByteStringLikeString extends ByteStringLike[String] {
    override def toByteString(t: String): ByteString = ByteString(t)
  }

  implicit object ByteStringLikeByteString extends ByteStringLike[ByteString] {
    override def toByteString(t: ByteString): ByteString = t
  }
}

object ByteStringConverters extends ByteStringConverters
