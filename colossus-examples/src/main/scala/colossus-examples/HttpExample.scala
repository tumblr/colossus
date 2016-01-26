package colossus.examples

import akka.util.ByteString
import colossus.IOSystem
import colossus.core.{DataBuffer, Server, ServerRef, WorkerRef}
import colossus.protocols.http._
import colossus.protocols.redis._
import colossus.service.{Callback, Service, ServiceClient}
import java.net.InetSocketAddress

import UrlParsing._
import HttpMethod._
import Callback.Implicits._

import colossus.controller.IteratorGenerator


object HttpExample {

  class HttpExampleService(redis: RedisCallbackClient, worker: WorkerRef) extends Service[Http] {
    
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
        connection accept new HttpExampleService(redis, worker)
      }
    }
  }

}
