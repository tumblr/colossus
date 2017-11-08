import java.net.{InetAddress, InetSocketAddress}

import akka.actor.ActorSystem
import akka.util.ByteString
import colossus.IOSystem
import colossus.metrics.logging.ColossusLogging
import colossus.protocols.http.Http
import colossus.protocols.http.HttpMethod._
import colossus.protocols.http.UrlParsing._
import colossus.protocols.http.{HttpServer, Initializer, RequestHandler}
import colossus.protocols.redis.Redis
import colossus.service.GenRequestHandler.PartialHandler
import colossus.service.Callback

import scala.concurrent.Future

object PlayingWithServiceClientFutures extends App with ColossusLogging {

  implicit val actorSystem = ActorSystem()
  implicit val ioSystem    = IOSystem()
  implicit val ec          = actorSystem.dispatcher

  HttpServer.start("example-server", 9000) { initContext =>
    new Initializer(initContext) {

      val hosts = Seq(
        new InetSocketAddress("localhost", 6379),
        new InetSocketAddress("localhost", 6379),
        new InetSocketAddress("localhost", 6379),
        new InetSocketAddress("localhost", 6379),
        new InetSocketAddress("localhost", 6379)
      )

      val redisClient = Redis.futureClient(hosts)

      override def onConnect =
        serverContext =>
          new RequestHandler(serverContext) {
            override def handle: PartialHandler[Http] = {

              case req @ Get on Root / "bang" =>
                val f = redisClient
                  .get(ByteString("bang"))
                  .map { data =>
                    req.ok(data)
                  }
                  .recoverWith {
                    case e => Future(req.ok(s"NOPE: $e"))
                  }

                Callback.fromFuture(f)
            }
        }
    }
  }

}
