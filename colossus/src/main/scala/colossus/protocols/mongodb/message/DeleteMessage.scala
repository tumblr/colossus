package colossus.protocols.mongodb.message

import akka.util.ByteString
import colossus.util.bson.BsonDocument

/**
 * The OP_DELETE message is used to remove one or more documents from a collection.
 * The format of the OP_DELETE message is:
 *
 * {{{
 * struct {
 *     MsgHeader header;             // standard message header
 *     int32     ZERO;               // 0 - reserved for future use
 *     cstring   fullCollectionName; // "dbname.collectionname"
 *     int32     flags;              // bit vector - see below for details.
 *     document  selector;           // query object.  See below for details.
 * }
 * }}}
 */
case class DeleteMessage(fullCollectionName: String,
                         selector: BsonDocument,
                         singleRemove: Boolean = false) extends Message {

  val flags = if (singleRemove) 0x00000001 else 0x00000000

  override val responseTo: Int = 0

  override val opCode: Int = 2006

  override def encodeBody(): ByteString = {
    val builder = ByteString.newBuilder
      .putInt(0) // ZERO
      .putBytes(fullCollectionName.getBytes("utf-8"))
      .putByte(0)
      .putInt(flags)
      .append(selector.encode())
    builder.result()
  }

}
