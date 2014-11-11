package colossus
package protocols.redis

import core._
import service._


import akka.util.{ByteString, ByteStringBuilder}

class RedisServerCodec extends Codec.ServerCodec[Command, Reply] {
  private var commandParser = RedisCommandParser.command
  def reset() {
    commandParser = RedisCommandParser.command
  }
  def encode(reply: Reply) = reply.raw
  def decode(data: DataBuffer): Option[Command] = commandParser.parse(data)
}
