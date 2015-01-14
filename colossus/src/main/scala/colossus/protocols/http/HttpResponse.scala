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
    StreamingHttpResponse(response.version, response.code, response.headers, pipe)
  }
}

trait ByteStringLike[T] {

  def toByteString(t : T) : ByteString

}

trait ByteStringConverters {

  implicit object ByteStringLikeString extends ByteStringLike[String] {
    override def toByteString(t: String): ByteString = ByteString(t)
  }
}

object ByteStringConverters extends ByteStringConverters
