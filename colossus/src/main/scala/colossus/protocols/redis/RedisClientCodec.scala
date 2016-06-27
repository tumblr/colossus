package colossus
package protocols.redis

import controller.StaticCodec
import core._
import service._

class RedisClientCodec() extends StaticCodec.Client[Redis] {
  private var replyParser = RedisReplyParser()

  def reset(){
    replyParser = RedisReplyParser()
  }

  def endOfStream = replyParser.endOfStream()

  def encode(cmd: Command, buffer: DataOutBuffer) { buffer.write(cmd.raw) }
  def decode(data: DataBuffer): Option[Reply] = replyParser.parse(data)
}
