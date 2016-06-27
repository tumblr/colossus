package colossus
package protocols.redis
package server

//TODO, make all this more generic and merge with HttpServer

import core.{InitContext, Server, ServerContext, ServerRef}
import service._
import Protocol.PartialHandler

class RedisServiceHandler(rh: RequestHandler) 
extends BasicServiceHandler[Redis](rh) {

  val codec = new RedisServerCodec

  def unhandledError = {
    case error => ErrorReply(error.toString)
  }

}

class RedisGenerator(context: InitContext) extends HandlerGenerator[Redis, RequestHandler](context) {

  def fullHandler = new RedisServiceHandler(_)

}

abstract class Initializer(context: InitContext) extends RedisGenerator(context) with ServiceInitializer[Redis, RequestHandler]

abstract class RequestHandler(ctx: ServerContext, config: ServiceConfig ) extends GenRequestHandler[Redis](config, ctx){
  def this(ctx: ServerContext) = this(ctx, ServiceConfig.load(ctx.name))
}

object RedisServer extends ServiceDSL[Redis, RequestHandler, Initializer] {

  def basicInitializer = new RedisGenerator(_)

}

