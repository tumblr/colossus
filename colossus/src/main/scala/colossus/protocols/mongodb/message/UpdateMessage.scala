package colossus.protocols.mongodb.message

import akka.util.ByteString
import colossus.util.bson.BsonDocument

/**
 * The OP_UPDATE message is used to update a document in a collection.
 * The format of a OP_UPDATE message is the following:
 *
 * {{{
 * struct OP_UPDATE {
 *     MsgHeader header;             // standard message header
 *     int32     ZERO;               // 0 - reserved for future use
 *     cstring   fullCollectionName; // "dbname.collectionname"
 *     int32     flags;              // bit vector. see below
 *     document  selector;           // the query to select the document
 *     document  update;             // specification of the update to perform
 * }
 * }}}
 */
case class UpdateMessage(fullCollectionName: String,
                         selector: BsonDocument,
                         update: BsonDocument,
                         upsert: Boolean = false,
                         multi: Boolean = false) extends Message {

  override val responseTo: Int = 0

  override val opCode: Int = 2001

  override def encodeBody(): ByteString = {
    val flags = (if (upsert) 0x00000001 else 0x00000000) | (if (multi) 0x00000002 else 0x00000000)

    ByteString.newBuilder
      .putInt(0) // ZERO
      .putBytes(fullCollectionName.getBytes("utf-8"))
      .putByte(0)
      .putInt(flags)
      .append(selector.encode())
      .append(update.encode())
      .result()
  }

}
