package colossus.protocols.mongodb.command

import colossus.util.bson.BsonDocument
import colossus.util.bson.BsonDsl._
import colossus.util.bson.Implicits._

case class Count(databaseName: String,
                 collectionName: String,
                 query: Option[BsonDocument] = None,
                 limit: Option[Int] = None,
                 skip: Option[Int] = None) extends Command {
  override val command: BsonDocument = {
    ("count" := collectionName) +
      ("query" := query) +
      ("limit" := limit) +
      ("skip" := skip)
  }
}
