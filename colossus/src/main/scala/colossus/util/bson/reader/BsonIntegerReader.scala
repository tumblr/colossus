package colossus.util.bson.reader

import java.nio.ByteBuffer

import colossus.util.bson.element.BsonInteger

case class BsonIntegerReader(buffer: ByteBuffer) extends Reader[BsonInteger] {

  def read: Option[BsonInteger] = {
    val name = readCString()
    val value = buffer.getInt()
    Some(BsonInteger(name, value))
  }
}
