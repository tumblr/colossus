package colossus
package protocols.redis

import core._
import service._

class RedisClientCodec() extends Codec.ClientCodec[Command, Reply] {
  private var replyParser = RedisReplyParser()

  def reset(){
    replyParser = RedisReplyParser()
  }

  def encode(cmd: Command) = DataBuffer(cmd.raw)
  def decode(data: DataBuffer): Option[DecodedResult[Reply]] = DecodedResult.static(replyParser.parse(data))
}
