package colossus.protocols.mongodb.message

import akka.util.ByteString

/**
 * The OP_KILL_CURSORS message is used to close an active cursor in the database.
 * This is necessary to ensure that database resources are reclaimed at the end of the query.
 * The format of the OP_KILL_CURSORS message is:
 *
 * {{{
 * struct {
 *     MsgHeader header;            // standard message header
 *     int32     ZERO;              // 0 - reserved for future use
 *     int32     numberOfCursorIDs; // number of cursorIDs in message
 *     int64*    cursorIDs;         // sequence of cursorIDs to close
 * }
 * }}}
 */
case class KillCursorsMessage(cursorIDs: Long*) extends Message {

  override val responseTo: Int = 0

  override val opCode: Int = 2007

  override def encodeBody(): ByteString = {
    val builder = ByteString.newBuilder
      .putInt(0) // ZERO
      .putInt(cursorIDs.size)

    cursorIDs.foreach(builder.putLong)

    builder.result()
  }

}
