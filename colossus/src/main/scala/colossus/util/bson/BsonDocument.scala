package colossus.util.bson

import akka.util.{ByteString, ByteStringBuilder}
import colossus.util.bson.element.BsonElement

case class BsonDocument(elements: BsonElement*) extends BsonValue {

  override def encode(): ByteString = {
    val builder = elements.foldLeft(new ByteStringBuilder) { (builder, element) =>
      builder.append(element.encode())
    }
    builder.putByte(0)

    ByteString.newBuilder
      .putInt(builder.length + 4)
      .append(builder.result())
      .result()
  }

  def +(element: BsonElement): BsonDocument = BsonDocument(elements :+ element)

  def +(element: Option[BsonElement]): BsonDocument = element match {
    case Some(element) => BsonDocument(elements :+ element)
    case None => this
  }

  def ++(that: BsonDocument): BsonDocument = BsonDocument(elements ++ that.elements)

  def ++(that: Option[BsonDocument]): BsonDocument = that match {
    case Some(that) => BsonDocument(elements ++ that.elements)
    case None => this
  }

  def getAs[T](key: String): Option[T] = {
    elements.find(_.name == key).map(_.value.asInstanceOf[Identifiable[T]].identifier)
  }

  override def toString(): String = s"{ ${elements.mkString(", ")} }"
}

object BsonDocument {
  def apply(elements: TraversableOnce[BsonElement]): BsonDocument = BsonDocument(elements.toSeq: _*)
}

