package colossus.util.bson.element

import colossus.util.bson.Implicits.BsonValueArray

case class BsonArray(name: String, value: BsonValueArray) extends BsonElement {
  val code: Byte = 0x04
}
