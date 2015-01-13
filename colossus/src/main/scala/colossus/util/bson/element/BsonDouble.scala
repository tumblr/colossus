package colossus.util.bson.element

import colossus.util.bson.Implicits.BsonValueDouble

case class BsonDouble(name: String, value: BsonValueDouble) extends BsonElement {
  val code: Byte = 0x01
}
