package colossus
package protocols.redis

import controller.StaticCodec
import core._
import service._

class RedisServerCodec extends StaticCodec.Server[Redis] {
  private var commandParser = RedisCommandParser.command
  def reset() {
    commandParser = RedisCommandParser.command
  }
  def encode(reply: Reply, buffer: DataOutBuffer) = buffer.write(reply.raw)
  def decode(data: DataBuffer): Option[Command] = commandParser.parse(data)

  def endOfStream() = None
}
