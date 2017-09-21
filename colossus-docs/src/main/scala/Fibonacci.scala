import akka.actor.ActorSystem
import colossus.IOSystem
import colossus.metrics.MetricSystem
import colossus.protocols.http.Http
import colossus.protocols.http.HttpMethod._
import colossus.protocols.http.UrlParsing._
import colossus.protocols.http.{HttpServer, Initializer, RequestHandler}
import colossus.service.Callback
import colossus.service.GenRequestHandler.PartialHandler

object Fibonacci extends App {
  // #fibonacci
  def fibonacci(i: Long): Long = i match {
    case 1 | 2 => 1
    case n     => fibonacci(n - 1) + fibonacci(n - 2)
  }

  implicit val actorSystem = ActorSystem()
  implicit val io          = IOSystem("io-system", workerCount = Some(1), MetricSystem("io-system"))

  HttpServer.start("example-server", 9000) { initContext =>
    new Initializer(initContext) {
      override def onConnect = serverContext => new RequestHandler(serverContext) {
        override def handle: PartialHandler[Http] = {

          case req @ Get on Root / "hello" =>
            Callback.successful(req.ok("Hello World!"))

          case req @ Get on Root / "fib" / Long(n) =>
            if (n > 0) {
              Callback.successful(req.ok(fibonacci(n).toString))
            } else {
              Callback.successful(req.badRequest("number must be positive"))
            }
        }
      }
    }
  }
  // #fibonacci
}
