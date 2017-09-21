import akka.actor.ActorSystem
import akka.util.ByteString
import colossus.IOSystem
import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.UrlParsing.{Root, on}
import colossus.protocols.http.{HttpServer, Initializer, RequestHandler}
import colossus.protocols.http.Http
import colossus.protocols.memcache.Memcache
import colossus.service.GenRequestHandler.PartialHandler

object MemcacheClient extends App {
  implicit val actorSystem = ActorSystem()
  implicit val ioSystem    = IOSystem()

  // #example
  HttpServer.start("example-server", 9000) { initContext =>
    new Initializer(initContext) {

      val memcacheClient = Memcache.client("localhost", 11211)

      override def onConnect = serverContext => new RequestHandler(serverContext) {
        override def handle: PartialHandler[Http] = {
          case request @ Get on Root =>
            val asyncResult = memcacheClient.get(ByteString("1"))
            asyncResult.map {
              case Some(reply) => request.ok(reply.data.utf8String)
              case None        => request.notFound("")
            }
        }
      }
    }
  }
  // #example
}
