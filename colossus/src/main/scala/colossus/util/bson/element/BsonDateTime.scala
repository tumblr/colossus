package colossus.util.bson.element

import colossus.util.bson.Implicits.BsonValueDateTime

case class BsonDateTime(name: String, value: BsonValueDateTime) extends BsonElement {
  val code: Byte = 0x09
}
