package colossus
package protocols.redis

import core._
import service._

import colossus.parsing.DataSize

class RedisClientCodec(maxSize: DataSize = RedisReplyParser.DefaultMaxSize) extends Codec.ClientCodec[Command, Reply] {
  private var replyParser = RedisReplyParser(maxSize)

  def reset(){
    replyParser = RedisReplyParser(maxSize)
  }

  def encode(cmd: Command) = DataBuffer(cmd.raw)
  def decode(data: DataBuffer): Option[DecodedResult[Reply]] = DecodedResult.static(replyParser.parse(data))
}
