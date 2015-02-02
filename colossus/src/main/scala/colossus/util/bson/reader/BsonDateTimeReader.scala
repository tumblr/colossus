package colossus.util.bson.reader

import java.nio.ByteBuffer

import colossus.util.bson.element.BsonDateTime
import org.joda.time.DateTime

case class BsonDateTimeReader(buffer: ByteBuffer) extends Reader[BsonDateTime] {
  override def read: Option[BsonDateTime] = {
    val name = readCString()
    val value = buffer.getLong()
    Some(BsonDateTime(name, new DateTime(value)))
  }
}
