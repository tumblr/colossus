import akka.actor.ActorSystem
import akka.util.ByteString
import colossus.IOSystem
import colossus.protocols.http.Http
import colossus.protocols.http.HttpMethod._
import colossus.protocols.http.UrlParsing._
import colossus.protocols.http.{HttpServer, Initializer, RequestHandler}
import colossus.protocols.redis.Redis
import colossus.service.GenRequestHandler.PartialHandler

import scala.util.{Failure, Success}

object RedisClient extends App {

  implicit val actorSystem = ActorSystem()
  implicit val ioSystem    = IOSystem()

  // #redis-client
  HttpServer.start("example-server", 9000) { initContext =>
    new Initializer(initContext) {

      val redisClient = Redis.client("localhost", 6379)

      override def onConnect = serverContext => new RequestHandler(serverContext) {
        override def handle: PartialHandler[Http] = {

          case req @ Get on Root / "get" / key => {
            redisClient.get(ByteString(key)).mapTry {
              case Success(data) => Success(req.ok(data.utf8String))
              case Failure(_)    => Success(req.notFound(s"Key $key was not found"))
            }
          }

          case req @ Get on Root / "set" / key / value => {
            redisClient.set(ByteString(key), ByteString(value)).map { _ =>
              req.ok("OK")
            }
          }
        }
      }
    }
  }
  // #redis-client

}
