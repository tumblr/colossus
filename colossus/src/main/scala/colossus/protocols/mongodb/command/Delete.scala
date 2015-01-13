package colossus.protocols.mongodb.command

import colossus.util.bson.BsonDocument
import colossus.util.bson.BsonDsl._
import colossus.util.bson.Implicits._

case class Delete(databaseName: String, collectionName: String, deletes: Seq[BsonDocument]) extends Command {
  override val command: BsonDocument = ("delete" := collectionName) + ("deletes" := array(deletes: _*))
}
