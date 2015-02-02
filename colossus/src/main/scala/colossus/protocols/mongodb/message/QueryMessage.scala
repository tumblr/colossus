package colossus.protocols.mongodb.message

import akka.util.ByteString
import colossus.util.bson.BsonDocument
import colossus.util.bson.BsonDsl._
import colossus.util.bson.Implicits._

/**
 * The OP_QUERY message is used to query the database for documents in a collection.
 * The format of the OP_QUERY message is:
 *
 * {{{
 * struct OP_QUERY {
 *     MsgHeader header;                 // standard message header
 *     int32     flags;                  // bit vector of query options.  See below for details.
 *     cstring   fullCollectionName;     // "dbname.collectionname"
 *     int32     numberToSkip;           // number of documents to skip
 *     int32     numberToReturn;         // number of documents to return in the first OP_REPLY batch
 *     document  query;                  // query object.  See below for details.
 *   [ document  returnFieldsSelector; ] // Optional. Selector indicating the fields to return.
 * }
 * }}}
 */
case class QueryMessage(fullCollectionName: String,
                        query: BsonDocument,
                        fields: Seq[String] = Seq.empty[String],
                        numberToSkip: Int = 0,
                        numberToReturn: Int = 0) extends Message {

  val flags: Int = 0

  override val responseTo: Int = 0

  override val opCode: Int = 2004

  override def encodeBody(): ByteString = {
    val builder = ByteString.newBuilder
      .putInt(flags)
      .putBytes(fullCollectionName.getBytes("utf-8"))
      .putByte(0)
      .putInt(numberToSkip)
      .putInt(numberToReturn)
      .append(query.encode())

    if (fields.nonEmpty) {
      builder.append(document(fields.map(_ := 1): _*).encode())
    }

    builder.result()
  }

}
