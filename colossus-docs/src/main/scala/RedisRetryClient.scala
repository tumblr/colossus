import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.util.ByteString
import colossus.IOSystem
import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.Http
import colossus.protocols.http.UrlParsing.{Root, on}
import colossus.protocols.http.{HttpServer, Initializer, RequestHandler}
import colossus.protocols.redis.Redis
import colossus.service.GenRequestHandler.PartialHandler
import colossus.service.LoadBalancingClient

object RedisRetryClient extends App {

  implicit val actorSystem = ActorSystem()
  implicit val ioSystem    = IOSystem()

  // #example
  HttpServer.start("example-server", 9000) { initContext =>
    new Initializer(initContext) {

      val generator = (address: InetSocketAddress) => {
        Redis.client(address.getHostName, address.getPort)
      }

      val addresses                 = List.fill(3)(new InetSocketAddress("localhost", 6379))
      val loadBalancingRedisClients = new LoadBalancingClient[Redis](worker, generator, 3, addresses)
      val redisClient               = Redis.client(loadBalancingRedisClients)

      override def onConnect = serverContext => new RequestHandler(serverContext) {
        override def handle: PartialHandler[Http] = {
          case request @ Get on Root =>
            redisClient.append(ByteString("key"), ByteString("VALUE")).map { result =>
              request.ok(s"Length of key is $result")
            }
        }
      }
    }
  }
  // #example
}
