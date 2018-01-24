import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.util.ByteString
import colossus.core.{BackoffMultiplier, BackoffPolicy, IOSystem}
import colossus.metrics.MetricAddress
import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.Http
import colossus.protocols.http.UrlParsing.{Root, on}
import colossus.protocols.http.{HttpServer, Initializer, RequestHandler}
import colossus.protocols.redis.Redis
import colossus.service.ClientConfig
import colossus.service.GenRequestHandler.PartialHandler

import scala.concurrent.duration._

object RedisRetryClient extends App {

  implicit val actorSystem = ActorSystem()
  implicit val ioSystem    = IOSystem()

  // #example
  HttpServer.start("example-server", 9000) { initContext =>
    new Initializer(initContext) {

      val config = ClientConfig(
        address = Seq(new InetSocketAddress("localhost", 6379)),
        requestTimeout = 1.second,
        name = MetricAddress.Root / "redis",
        requestRetry = BackoffPolicy(
          baseBackoff = 0.milliseconds,
          multiplier = BackoffMultiplier.Constant,
          maxTries = Some(3)
        )
      )

      val redisClient = Redis.client(config)

      override def onConnect: RequestHandlerFactory =
        serverContext =>
          new RequestHandler(serverContext) {
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
