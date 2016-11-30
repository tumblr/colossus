package colossus
package protocols.http

import akka.util.ByteString
import scala.util.Try

class HttpBody(private val body: Array[Byte], val contentType : Option[HttpHeader] = None)  {

  def size = body.length

  def encode(buffer: core.DataOutBuffer) {
    if (size > 0) buffer.write(body)
  }

  def bytes: ByteString = ByteString(body)

  def as[T](implicit decoder: HttpBodyDecoder[T]): Try[T] = decoder.decode(body)

  override def equals(that: Any) = that match {
    case that: HttpBody => that.bytes == this.bytes
    case other => false
  }

  override def hashCode = body.hashCode

  override def toString = bytes.utf8String

  def withContentType(contentType: String) = withContentTypeHeader(HttpHeader("Content-Type", contentType))

  def withContentTypeHeader(header: HttpHeader) = new HttpBody(body, Some(header))

}

/**
 * A Typeclass to decode a raw http body into some specific type
 */
trait HttpBodyDecoder[T] {

  //maybe somehow incorporate checking the content-type header?

  def decode(body: Array[Byte]): Try[T]

}

trait HttpBodyDecoders {

  implicit object StringDecoder extends HttpBodyDecoder[String] {
    def decode(body: Array[Byte]) = Try {
      new String(body)
    }
  }

  implicit object ByteStringDecoder extends HttpBodyDecoder[ByteString] {
    def decode(body: Array[Byte]) = Try {
      ByteString(body)
    }
  }

  implicit object ArrayDecoder extends HttpBodyDecoder[Array[Byte]] {
    def decode(body: Array[Byte]) = Try { body }
  }

}

trait HttpBodyEncoder[T] {

  def encode(data: T): HttpBody

  def contentTypeHeader(contentType: String): HttpHeader = HttpHeader("Content-Type", contentType)

}

trait HttpBodyEncoders {
  implicit object ByteStringEncoder extends HttpBodyEncoder[ByteString] {
    def encode(data: ByteString): HttpBody = new HttpBody(data.toArray)
  }

  implicit object StringEncoder extends HttpBodyEncoder[String] {
    val ctype = HttpHeader("Content-Type", "text/plain")
    def encode(data: String) : HttpBody = new HttpBody(data.getBytes("UTF-8"), Some(ctype))
  }

  implicit object IdentityEncoder extends HttpBodyEncoder[HttpBody] {
    def encode(b: HttpBody) = b
  }
}

object HttpBody extends HttpBodyEncoders {

  val NoBody = new HttpBody(Array(), None)

  def apply[T](data: T)(implicit encoder: HttpBodyEncoder[T]): HttpBody = encoder.encode(data)

  def apply[T](data: T, contentType: String)(implicit encoder: HttpBodyEncoder[T]) = {
    encoder.encode(data).withContentType(contentType)
  }

  def apply[T](data: T, contentTypeHeader: HttpHeader)(implicit encoder: HttpBodyEncoder[T]) = {
    encoder.encode(data).withContentTypeHeader(contentTypeHeader)
  }

}
