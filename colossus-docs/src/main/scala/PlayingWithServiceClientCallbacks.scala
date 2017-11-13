import java.net.InetSocketAddress

import akka.actor.Actor.Receive
import akka.actor.ActorSystem
import akka.util.ByteString
import colossus.IOSystem
import colossus.metrics.logging.ColossusLogging
import colossus.protocols.http.HttpMethod._
import colossus.protocols.http.UrlParsing._
import colossus.protocols.http.{Http, HttpServer, Initializer, RequestHandler}
import colossus.protocols.redis.Redis
import colossus.service.GenRequestHandler.PartialHandler

import scala.util.{Failure, Success, Try}

object PlayingWithServiceClientCallbacks extends App with ColossusLogging {

  implicit val actorSystem = ActorSystem()
  implicit val ioSystem    = IOSystem()
  implicit val ec          = actorSystem.dispatcher

  val serverRef = HttpServer.start("example-server", 9000) { initContext =>
    new Initializer(initContext) {

      val hosts = Seq(
        new InetSocketAddress("localhost", 6379),
        new InetSocketAddress("localhost", 6379),
        new InetSocketAddress("localhost", 6379),
        new InetSocketAddress("localhost", 6379),
        new InetSocketAddress("localhost", 6379)
      )

      val redisClient = Redis.client(hosts)

      override def receive: Receive = {
        case _ => redisClient.update(hosts)
      }

      override def onConnect =
        serverContext =>
          new RequestHandler(serverContext) {
            override def handle: PartialHandler[Http] = {

              case req @ Get on Root / "bang" =>
                redisClient.get(ByteString("bang")).mapTry {
                  case Success(data) => Try(req.ok(data))
                  case Failure(e)    => Try(req.ok(s"NOPE: $e"))
                }
            }
        }
    }
  }

  while (true) {
    Thread.sleep(5000)
    serverRef.initializerBroadcast(Seq(new InetSocketAddress("localhost", 6379)))
  }
}
