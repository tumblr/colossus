package colossus
package protocols.redis

import service.{BasicServiceDSL, ProcessingFailure}

object server extends BasicServiceDSL[colossus.protocols.redis.Redis] {

  protected def provideCodec = new RedisServerCodec

  protected def errorMessage(reason: ProcessingFailure[Command]) = ErrorReply(reason.toString)

  val RedisServer = Server

}
