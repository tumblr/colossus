package colossus.examples

import java.net.InetSocketAddress

import akka.util.ByteString
import colossus.IOSystem
import colossus.core.{Initializer, Server, ServerContext, ServerRef}
import colossus.protocols.http.HttpMethod._
import colossus.protocols.http.UrlParsing._
import colossus.protocols.http._
import colossus.protocols.redis.Redis.defaults._
import colossus.protocols.redis._
import colossus.service.Callback.Implicits._
import colossus.service.{Callback, ServiceConfig}

import scala.concurrent.duration._

object HttpExample {

  class HttpExampleService(redis: RedisClient[Callback], context: ServerContext) extends HttpService(ServiceConfig.Default, context){

    def invalidReply(reply: Reply) = s"Invalid reply from redis $reply"

    def handle = {
      case req @ Get on Root => req.ok("Hello World!")

      case req @ Get on Root / "shutdown" => {
        worker.system.actorSystem.shutdown
        req.ok("bye")
      }

      case req @ Get on Root / "close" => {
        disconnect()
        req.ok("closing")
      }

      case req @ Get on Root / "echo" => {
        req.ok(req.toString)
      }

      case req @ Get on Root / "get"  / key => redis.get(ByteString(key)).map{x => req.ok(x.utf8String)}

      case req @ Get on Root / "set" / key / value => redis.set(ByteString(key), ByteString(value)).map{ x =>
        req.ok(x.toString)
      }

    }

  }


  def start(port: Int, redisAddress: InetSocketAddress)(implicit system: IOSystem): ServerRef = {
    Server.start("http-example", port){implicit worker => new Initializer(worker) {

      val redis: RedisClient[Callback] = Redis.client(redisAddress.getHostName, redisAddress.getPort, 1.second)

      def onConnect = context => new HttpExampleService(redis, context)
    }}
  }

}
