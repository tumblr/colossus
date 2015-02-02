package colossus.protocols.mongodb.message

import akka.util.ByteString
import colossus.util.bson.BsonDocument

/**
 * The OP_INSERT message is used to insert one or more documents into a collection.
 * The format of the OP_INSERT message is:
 *
 * {{{
 * struct {
 *     MsgHeader header;             // standard message header
 *     int32     flags;              // bit vector - see below
 *     cstring   fullCollectionName; // "dbname.collectionname"
 *     document* documents;          // one or more documents to insert into the collection
 * }
 * }}}
 */
case class InsertMessage(fullCollectionName: String,
                         documents: Seq[BsonDocument],
                         continueOnError: Boolean = false) extends Message {

  val flags = if (continueOnError) 0x00000001 else 0x00000000

  override val responseTo: Int = 0

  override val opCode: Int = 2002

  override def encodeBody(): ByteString = {
    val builder = ByteString.newBuilder
      .putInt(flags)
      .putBytes(fullCollectionName.getBytes("utf-8"))
      .putByte(0)
    documents.foreach(builder append _.encode())
    builder.result()
  }

}
