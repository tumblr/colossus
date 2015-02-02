package colossus.util.bson.reader

import java.nio.ByteBuffer
import java.nio.ByteOrder._

import akka.util.ByteString
import colossus.util.bson.BsonDocument
import colossus.util.bson.element.BsonElement

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

case class BsonDocumentReader(buffer: ByteBuffer) extends Reader[BsonDocument] {

  buffer.order(LITTLE_ENDIAN)

  private val elements: ArrayBuffer[Option[BsonElement]] = new ArrayBuffer[Option[BsonElement]]

  def readElement(code: Byte): Option[BsonElement] = code match {
    case 0x01 => BsonDoubleReader(buffer).read
    case 0x02 => BsonStringReader(buffer).read
    case 0x03 => BsonObjectReader(buffer).read
    case 0x04 => BsonArrayReader(buffer).read
    case 0x07 => BsonObjectIdReader(buffer).read
    case 0x08 => BsonBooleanReader(buffer).read
    case 0x09 => BsonDateTimeReader(buffer).read
    case 0x0A => BsonNullReader(buffer).read
    case 0x10 => BsonIntegerReader(buffer).read
    case 0x12 => BsonLongReader(buffer).read
  }

  override def read: Option[BsonDocument] = {
    val size = buffer.getInt()

    breakable {
      while (buffer.hasRemaining) {
        val code = buffer.get()
        if (code != 0x00) {
          elements += readElement(code)
        } else {
          break
        }
      }
    }

    Some(BsonDocument(elements.flatten: _*))
  }
}

object BsonDocumentReader {

  def apply(array: Array[Byte]): BsonDocumentReader = BsonDocumentReader(ByteBuffer.wrap(array))

  def apply(buffer: ByteString): BsonDocumentReader = BsonDocumentReader(buffer.asByteBuffer)
}