package colossus.protocols.mongodb

import colossus.core.{DataBuffer, DataReader}
import colossus.protocols.mongodb.message.{Message, Reply}
import colossus.service.{Codec, DecodedResult}

class MongoClientCodec extends Codec.ClientCodec[Message, Reply] {

  /**
   * Decode a single object from a bytestream.
   */
  override def decode(data: DataBuffer): Option[Decoded] = {
    DecodedResult.static(Reply.decode(data.data))
  }

  override def encode(out: Message): DataReader = DataBuffer(out.encode())

  override def reset(): Unit = {}
}
