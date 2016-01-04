package colossus.examples

import akka.util.ByteString
import colossus.IOSystem
import colossus.core.{DataBuffer, ServerRef, WorkerRef}
import colossus.protocols.http._
import colossus.protocols.redis._
import colossus.service.{Callback, Service}
import java.net.InetSocketAddress

import UrlParsing._
import HttpMethod._
import Callback.Implicits._

import colossus.controller.IteratorGenerator


object HttpExample {

  /**
   * Here we're demonstrating a common way of breaking out the business logic
   * from the server setup, which makes functional testing easy
   */
  class HttpRoutes(redis: RedisCallbackClient, worker: WorkerRef) {
    
    def invalidReply(reply: Reply) = s"Invalid reply from redis $reply"    

    def handler: PartialFunction[HttpRequest, Callback[HttpResponse]] = {
      case req @ Get on Root => req.ok("Hello World!")

      case req @ Get on Root / "shutdown" => {
        worker.system.actorSystem.shutdown
        req.ok("bye")
      }

      case req @ Get on Root / "simstream" => {
        val body = ByteString("Hello World!")
        HttpResponse(
          HttpResponseHead(HttpVersion.`1.1`, HttpCodes.OK, Vector(HttpResponseHeader("content-length",body.size.toString))), 
          HttpMessageBody.Stream(colossus.controller.Source.one(DataBuffer(body)))
        )
      }
        

      case req @ Get on Root / "stream" => {
        val source = new IteratorGenerator((0 to 10).map{x => DataBuffer(ByteString(x.toString))}.toIterator)
        val chunker = new ChunkEncodingPipe
        chunker.feed(source, true)
        HttpResponse(HttpResponseHead(HttpVersion.`1.1`, HttpCodes.OK, Vector(HttpResponseHeader("transfer-encoding","chunked"))), HttpMessageBody.Stream(chunker))
      }

      case req @ Get on Root / "get"  / key => redis.get(ByteString(key)).map{x => req.ok(x.utf8String)}

      case req @ Get on Root / "set" / key / value => redis.set(ByteString(key), ByteString(value)).map{ x =>
        req.ok(x.toString)
      }

    }

  }


  def start(port: Int, redisAddress: InetSocketAddress)(implicit system: IOSystem): ServerRef = {
    Service.serve[Http]("http-example", port){context =>
      val redis = new RedisCallbackClient(context.clientFor[Redis](redisAddress.getHostName, redisAddress.getPort))
      //because our routes object has no internal state, we can share it among
      //connections.  If the class did have some per-connection internal state,
      //we'd just create one per connection
      val routes = new HttpRoutes(redis, context.worker)

      context.handle{connection => 
        connection.become(routes.handler)
      }
    }
  }

}
