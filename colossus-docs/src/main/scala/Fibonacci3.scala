import akka.actor.ActorSystem
import akka.util.ByteString
import colossus.IOSystem
import colossus.metrics.MetricSystem
import colossus.protocols.http.Http
import colossus.protocols.http.HttpMethod._
import colossus.protocols.http.UrlParsing._
import colossus.protocols.http.{HttpServer, Initializer, RequestHandler}
import colossus.protocols.memcache.Memcache
import colossus.service.Callback
import colossus.service.GenRequestHandler.PartialHandler

import scala.concurrent.Future

object Fibonacci3 extends App {

  def fibonacci(i: Long): Long = i match {
    case 1 | 2 => 1
    case n     => fibonacci(n - 1) + fibonacci(n - 2)
  }

  implicit val actorSystem      = ActorSystem()
  implicit val executionContext = actorSystem.dispatcher
  implicit val io               = IOSystem("io-system", workerCount = Some(1), MetricSystem("io-system"))

  // #fibonacci3
  HttpServer.start("example-server", 9000) { initContext =>
    new Initializer(initContext) {

      val cache = Memcache.client("localhost", 11211)

      override def onConnect = serverContext => new RequestHandler(serverContext) {
        override def handle: PartialHandler[Http] = {

          case req @ Get on Root / "hello" =>
            Callback.successful(req.ok("Hello World!"))

          case req @ Get on Root / "fib" / Long(n) =>
            if (n > 0) {
              val key = ByteString(s"fib_$n")
              cache.get(key).flatMap {
                case Some(value) => Callback.successful(req.ok(value.data.utf8String))
                case None =>
                  for {
                    result   <- Callback.fromFuture(Future(fibonacci(n)))
                    cacheSet <- cache.set(key, ByteString(result.toString))
                  } yield req.ok(result.toString)
              }
            } else {
              Callback.successful(req.badRequest("number must be positive"))
            }
        }
      }
    }
  }
  // #fibonacci3
}
