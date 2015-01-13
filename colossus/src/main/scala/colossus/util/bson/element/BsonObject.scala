package colossus.util.bson.element

import colossus.util.bson.Implicits.BsonValueObject

case class BsonObject(name: String, value: BsonValueObject) extends BsonElement {
  val code: Byte = 0x03
}
