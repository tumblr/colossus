package colossus.util.bson.reader

import java.nio.ByteBuffer

import colossus.util.bson.element.BsonString

case class BsonStringReader(buffer: ByteBuffer) extends Reader[BsonString] {

  def read: Option[BsonString] = {
    val name = readCString()
    val value = readString()
    Some(BsonString(name, value))
  }
}
