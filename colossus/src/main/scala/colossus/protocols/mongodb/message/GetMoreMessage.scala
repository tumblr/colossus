package colossus.protocols.mongodb.message

import akka.util.ByteString

/**
 * The OP_GET_MORE message is used to query the database for documents in a collection.
 * The format of the OP_GET_MORE message is:
 *
 * {{{
 * struct {
 *     MsgHeader header;             // standard message header
 *     int32     ZERO;               // 0 - reserved for future use
 *     cstring   fullCollectionName; // "dbname.collectionname"
 *     int32     numberToReturn;     // number of documents to return
 *     int64     cursorID;           // cursorID from the OP_REPLY
 * }
 * }}}
 */
case class GetMoreMessage(fullCollectionName: String,
                          cursorID: Long,
                          numberToReturn: Int = 0) extends Message {

  override val responseTo: Int = 0

  override val opCode: Int = 2005

  override def encodeBody(): ByteString = {
    ByteString.newBuilder
      .putInt(0) // ZERO
      .putBytes(fullCollectionName.getBytes("utf-8"))
      .putByte(0)
      .putInt(numberToReturn)
      .putLong(cursorID)
      .result()
  }

}
