package colossus.protocols.http

import akka.util.ByteString
import colossus.core.{DataBlock, DataOutBuffer}

import scala.util.Try

class HttpBody(private val body: Array[Byte]) {

  def size = body.length

  def encode(buffer: DataOutBuffer) {
    if (size > 0) buffer.write(body)
  }

  def bytes: ByteString      = ByteString(body)
  def asDataBlock: DataBlock = DataBlock(body)

  def as[T](implicit decoder: HttpBodyDecoder[T]): Try[T] = decoder.decode(body)

  override def equals(that: Any) = that match {
    case that: HttpBody => that.bytes == this.bytes
    case _              => false
  }

  override def hashCode = body.hashCode

  override def toString = bytes.utf8String
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

  def contentType: Option[String]

}

trait HttpBodyEncoders {
  implicit object ByteStringEncoder extends HttpBodyEncoder[ByteString] {
    val contentType = None
    def encode(data: ByteString): HttpBody = new HttpBody(data.toArray)
  }

  implicit object StringEncoder extends HttpBodyEncoder[String] {
    val contentType = Some(ContentType.TextPlain)
    def encode(data: String): HttpBody = new HttpBody(data.getBytes("UTF-8"))
  }

  implicit object IdentityEncoder extends HttpBodyEncoder[HttpBody] {
    val contentType = None
    def encode(b: HttpBody) = b
  }
}

object HttpBody extends HttpBodyEncoders {

  val NoBody = new HttpBody(Array.emptyByteArray)

  def apply[T](data: T)(implicit encoder: HttpBodyEncoder[T]): HttpBody = encoder.encode(data)

}
