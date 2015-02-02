package colossus.util.bson.reader

import java.nio.ByteBuffer

import colossus.util.bson.element.BsonDouble

case class BsonDoubleReader(buffer: ByteBuffer) extends Reader[BsonDouble] {

  def read: Option[BsonDouble] = {
    val name = readCString()
    val value = buffer.getDouble()
    Some(BsonDouble(name, value))
  }
}
