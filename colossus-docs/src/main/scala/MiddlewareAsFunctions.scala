import akka.actor.ActorSystem
import colossus.IOSystem
import colossus.protocols.http.{Http, HttpRequest, HttpResponse}
import colossus.protocols.http.HttpMethod._
import colossus.protocols.http.UrlParsing._
import colossus.protocols.http.{HttpServer, Initializer, RequestHandler}
import colossus.service.Callback
import colossus.service.GenRequestHandler.PartialHandler

object MiddlewareAsFunctions extends App {

  implicit val actorSystem = ActorSystem()
  implicit val ioSystem    = IOSystem()

  HttpServer.start("example-server", 9000) { initContext =>
    new Initializer(initContext) {
      override def onConnect = serverContext => new RequestHandler(serverContext) {
        override def handle: PartialHandler[Http] = {
          // #example1
          case request @ Post on Root / "shout" => withBody(request) { body =>
            Callback.successful(request.ok(body.toUpperCase))
          }
          // #example1
        }
      }
    }
  }

  // #example
  def withBody(req: HttpRequest)(f: String => Callback[HttpResponse]): Callback[HttpResponse] = {
    val bytes = req.body.bytes
    if (bytes.isEmpty) {
      Callback.successful(req.badRequest("Missing body"))
    } else {
      f(bytes.utf8String)
    }
  }
  // #example

}
