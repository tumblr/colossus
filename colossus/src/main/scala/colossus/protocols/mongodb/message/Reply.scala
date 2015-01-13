package colossus.protocols.mongodb.message

import java.nio.ByteBuffer

import akka.util.ByteString
import colossus.util.bson.BsonDocument
import colossus.util.bson.reader.BsonDocumentReader

import scala.collection.mutable.ArrayBuffer

/**
 * The OP_REPLY message is sent by the database in response to an OP_QUERY or OP_GET_MORE message.
 * The format of an OP_REPLY message is:
 *
 * {{{
 * struct {
 *     MsgHeader header;         // standard message header
 *     int32     responseFlags;  // bit vector - see details below
 *     int64     cursorID;       // cursor id if client needs to do get more's
 *     int32     startingFrom;   // where in the cursor this reply is starting
 *     int32     numberReturned; // number of documents in the reply
 *     document* documents;      // documents
 * }
 * }}}
 */
case class Reply(responseTo: Int,
                 cursorID: Long,
                 startingFrom: Int,
                 numberReturned: Int,
                 documents: Seq[BsonDocument]) extends Message {

  val flags = 0

  override def opCode: Int = 1

  override def encodeBody(): ByteString = {
    val builder = ByteString.newBuilder
      .putInt(flags)
      .putLong(cursorID)
      .putInt(startingFrom)
      .putInt(numberReturned)

    documents.foreach(doc => builder.append(doc.encode()))

    builder.result()
  }
}

object Reply {
  def decode(buffer: ByteBuffer): Option[Reply] = {
    val length = buffer.getInt()
    buffer.getInt() // requestID
    val responseTo = buffer.getInt()
    buffer.getInt() // opCode
    val flags = buffer.getInt()
    val cursorID = buffer.getLong()
    val startingFrom = buffer.getInt()
    val numberReturned = buffer.getInt()

    val documents = ArrayBuffer[BsonDocument]()
    val reader = BsonDocumentReader(buffer)

    while (buffer.hasRemaining()) {
      reader.read.map(document => documents += document)
    }

    Some(Reply(responseTo, cursorID, startingFrom, numberReturned, documents.toSeq))
  }
}
