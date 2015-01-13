package colossus.util.bson.reader

import java.nio.ByteBuffer

import colossus.util.bson.element.BsonNull

case class BsonNullReader(buffer: ByteBuffer) extends Reader[BsonNull] {
  override def read: Option[BsonNull] = {
    Some(BsonNull(readCString()))
  }
}
