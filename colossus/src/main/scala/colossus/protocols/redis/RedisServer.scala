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

  def receivedMessage(message: Any, sender: akka.actor.ActorRef){}


}

abstract class Initializer(context: InitContext) {
  
  implicit val worker = context.worker

  def onConnect : ServerContext => RequestHandler

}

abstract class RequestHandler(config: ServiceConfig, ctx: ServerContext) extends GenRequestHandler[Redis](config, ctx) {
  def this(ctx: ServerContext) = this(ServiceConfig.load(ctx.name), ctx)
}

object RedisServer {
  
  def start(name: String, port: Int)(init: InitContext => Initializer)(implicit io: IOSystem): ServerRef = {
    Server.start(name, port){i => new core.Initializer(i) {
      val rinit = init(i)
      def onConnect = ctx => new RedisServiceHandler(rinit.onConnect(ctx))
    }}
  }

  def basic(name: String, port: Int)(handler: PartialHandler[Redis])(implicit io: IOSystem) = start(name, port){new Initializer(_) {
    def onConnect = new RequestHandler(_) { def handle = handler }
  }}
}

