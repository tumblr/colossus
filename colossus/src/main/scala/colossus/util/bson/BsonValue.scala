package colossus.util.bson

import java.nio.ByteOrder

trait BsonValue extends Writable {
  implicit val byteOrder = ByteOrder.LITTLE_ENDIAN
}
