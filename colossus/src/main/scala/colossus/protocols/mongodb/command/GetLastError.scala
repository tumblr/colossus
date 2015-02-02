package colossus.protocols.mongodb.command

import colossus.util.bson.BsonDsl._
import colossus.util.bson.Implicits._
import colossus.util.bson.{BsonDocument, BsonValue}

case class GetLastError(databaseName: String,
                        j: Option[Boolean] = None,
                        w: Option[BsonValue] = None,
                        fsync: Option[Boolean] = None,
                        wtimeout: Option[Int] = None) extends Command {
  override val command: BsonDocument = {
    ("getLastError" := 1) +
      ("j" := j) +
      ("w" := w) +
      ("fsync" := fsync) +
      ("wtimeout" := wtimeout)
  }
}
