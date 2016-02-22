package colossus.examples

import akka.util.ByteString
import colossus.IOSystem
import colossus.core.{DataBuffer, Server, ServerRef, WorkerRef}
import colossus.protocols.http._
import colossus.protocols.redis._
import colossus.service.{Callback, Service, ServiceClient, ServiceConfig}
import java.net.InetSocketAddress

import UrlParsing._
import HttpMethod._
import Callback.Implicits._

import colossus.controller.IteratorGenerator

import scala.concurrent.duration._


object HttpExample {

  class HttpExampleService(redis: RedisCallbackClient)(implicit io: IOSystem) extends Service[Http](ServiceConfig("/foo", 1.second, requestMetrics = false)){
    
    def invalidReply(reply: Reply) = s"Invalid reply from redis $reply"    

    def handle = {
      case req @ Get on Root => req.ok("Hello World!")

      case req @ Get on Root / "shutdown" => {
        io.actorSystem.shutdown
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
    Server.start("http-example", port){context =>
      import context.worker
      val redis = new RedisCallbackClient(ServiceClient[Redis](redisAddress.getHostName, redisAddress.getPort))

      context onConnect {connection => 
        connection accept new HttpExampleService(redis)
      }
    }
  }

}
