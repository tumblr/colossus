package colossus.examples

import java.net.InetSocketAddress

import akka.util.ByteString
import colossus.IOSystem
import colossus.core.{ServerContext, ServerRef}
import colossus.protocols.http.HttpMethod._
import colossus.protocols.http.UrlParsing._
import colossus.protocols.http._
import colossus.protocols.redis._
import colossus.service.Callback.Implicits._
import colossus.service.Callback

import scala.concurrent.duration._

object HttpExample {

  class HttpExampleHandler(redis: RedisClient[Callback], context: ServerContext) extends RequestHandler(context) {

    def invalidReply(reply: Reply) = s"Invalid reply from redis $reply"

    def handle = {
      case req @ Get on Root => req.ok("Hello World!")

      case req @ Get on Root / "shutdown" => {
        server.system.actorSystem.terminate()
        req.ok("bye")
      }

      case req @ Get on Root / "close" => {
        disconnect()
        req.ok("closing")
      }

      case req @ Get on Root / "echo" => {
        req.ok(req.toString)
      }

      case req @ Get on Root / "delay" => {
        Callback.schedule(500.milliseconds)(req.ok("OK"))
      }

      case req @ Get on Root / "get" / key =>
        redis.get(ByteString(key)).map { x =>
          req.ok(x.utf8String)
        }

      case req @ Get on Root / "set" / key / value =>
        redis.set(ByteString(key), ByteString(value)).map { x =>
          req.ok(x.toString)
        }

    }

  }

  def start(port: Int, redisAddress: InetSocketAddress)(implicit system: IOSystem): ServerRef = {
    HttpServer.start("http-example", port) { init =>
      new Initializer(init) {

        val redis: RedisClient[Callback] = Redis.client(redisAddress.getHostName, redisAddress.getPort, 1.second)

        def onConnect = context => new HttpExampleHandler(redis, context)
      }
    }
  }

}
