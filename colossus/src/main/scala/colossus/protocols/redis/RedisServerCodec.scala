package colossus.protocols.redis

import colossus.controller.Codec
import colossus.core._

class RedisServerCodec extends Codec.Server[Redis] {
  private var commandParser = RedisCommandParser.command
  def reset() {
    commandParser = RedisCommandParser.command
  }
  def encode(reply: Reply, buffer: DataOutBuffer) = buffer.write(reply.raw)
  def decode(data: DataBuffer): Option[Command]   = commandParser.parse(data)

  def endOfStream() = None
}
