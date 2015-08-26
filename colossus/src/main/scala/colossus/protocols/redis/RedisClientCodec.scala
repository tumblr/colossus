package colossus
package protocols.redis

import core._
import service._

import colossus.parsing.DataSize
import encoding.Encoders

class RedisClientCodec(maxSize: DataSize = RedisReplyParser.DefaultMaxSize) extends Codec.ClientCodec[Command, Reply] {
  private var replyParser = RedisReplyParser(maxSize)

  def reset(){
    replyParser = RedisReplyParser(maxSize)
  }

  def encode(cmd: Command) = Encoders.unsized{DataBuffer(cmd.raw)}
  def decode(data: DataBuffer): Option[DecodedResult[Reply]] = DecodedResult.static(replyParser.parse(data))
}
