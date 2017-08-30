import akka.actor.ActorSystem
import akka.util.ByteString
import colossus.IOSystem
import colossus.protocols.http.Http
import colossus.protocols.http.HttpMethod._
import colossus.protocols.http.UrlParsing._
import colossus.protocols.http.{HttpServer, Initializer, RequestHandler}
import colossus.protocols.redis.Redis
import colossus.service.GenRequestHandler.PartialHandler

object RedisClientExample extends App {

  implicit val actorSystem = ActorSystem()
  implicit val ioSystem    = IOSystem()

  // #example
  HttpServer.start("example-server", 9000) { initContext =>
    new Initializer(initContext) {

      val redisClient = Redis.client("localhost", 6379)

      override def onConnect = serverContext => new RequestHandler(serverContext) {
        override def handle: PartialHandler[Http] = {

          case request @ Get on Root / "get" / key => {
            val asyncResult = redisClient.get(ByteString("1"))
            asyncResult.map {
              case bytes => request.ok(bytes.utf8String)
            }
          }
        }
      }
    }
  }
  // #example

}
