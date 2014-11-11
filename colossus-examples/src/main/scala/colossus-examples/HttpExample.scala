package colossus.examples

import akka.util.ByteString
import colossus.IOSystem
import colossus.core.ServerRef
import colossus.protocols.http._
import colossus.protocols.redis._
import colossus.service.{LocalClient, Service, Response}
import java.net.InetSocketAddress

import UrlParsing._
import HttpMethod._
import UnifiedProtocol._


object HttpExample {

  /**
   * Here we're demonstrating a common way of breaking out the business logic
   * from the server setup, which makes functional testing easy
   */
  class HttpRoutes(redis: LocalClient[Command, Reply]) {
    
    def invalidReply(reply: Reply) = s"Invalid reply from redis $reply"    

    val handler: PartialFunction[HttpRequest, Response[HttpResponse]] = {
      case req @ Get on Root => req.ok("Hello World!")

      case req @ Get on Root / "get"  / key => redis.send(Commands.Get(ByteString(key))).map{
        case BulkReply(data) => req.ok(data.utf8String)
        case NilReply => req.notFound("(nil)")
        case other => req.error(invalidReply(other))
      }

      case req @ Get on Root / "set" / key / value => redis.send(Commands.Set(ByteString(key), ByteString(value))).map{
        case StatusReply(msg) => req.ok(msg)
        case other => req.error(invalidReply(other))
      }

    }

  }


  def start(port: Int, redisAddress: InetSocketAddress)(implicit system: IOSystem): ServerRef = {
    Service.serve[Http]("http-example", port){context =>
      val redis = context.clientFor[Redis](redisAddress.getHostName, redisAddress.getPort)
      //because our routes object has no internal state, we can share it among
      //connections.  If the class did have some per-connection internal state,
      //we'd just create one per connection
      val routes = new HttpRoutes(redis)

      context.handle{connection => 
        connection.become(routes.handler)
      }
    }
  }

}
