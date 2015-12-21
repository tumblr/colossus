package colossus
package protocols.redis

import core._
import service._

class RedisServerCodec extends Codec.ServerCodec[Command, Reply] {
  private var commandParser = RedisCommandParser.command
  def reset() {
    commandParser = RedisCommandParser.command
  }
  def encode(reply: Reply) = reply.raw
  def decode(data: DataBuffer): Option[DecodedResult[Command]] = DecodedResult.static(commandParser.parse(data))
}
