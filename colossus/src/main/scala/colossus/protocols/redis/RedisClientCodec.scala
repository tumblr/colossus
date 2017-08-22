package colossus.protocols.redis

import colossus.controller.Codec
import colossus.core._

class RedisClientCodec() extends Codec.Client[Redis] {
  private var replyParser = RedisReplyParser()

  def reset() {
    replyParser = RedisReplyParser()
  }

  def endOfStream = replyParser.endOfStream()

  def encode(cmd: Command, buffer: DataOutBuffer) { buffer.write(cmd.raw) }
  def decode(data: DataBuffer): Option[Reply] = replyParser.parse(data)
}
