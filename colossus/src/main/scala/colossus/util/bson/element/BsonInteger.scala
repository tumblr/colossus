package colossus.util.bson.element

import colossus.util.bson.Implicits.BsonValueInteger

case class BsonInteger(name: String, value: BsonValueInteger) extends BsonElement {
  val code: Byte = 0x10
}
