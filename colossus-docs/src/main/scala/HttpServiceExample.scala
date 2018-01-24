// #example
import akka.actor.ActorSystem
import colossus.core.IOSystem
import colossus.protocols.http.Http
import colossus.protocols.http.HttpMethod._
import colossus.protocols.http.UrlParsing._
import colossus.protocols.http.{HttpServer, Initializer, RequestHandler}
import colossus.service.Callback
import colossus.service.GenRequestHandler.PartialHandler

object HttpServiceExample extends App {

  implicit val actorSystem = ActorSystem()
  implicit val ioSystem    = IOSystem()

  HttpServer.start("example-server", 9000) { initContext =>
    new Initializer(initContext) {
      override def onConnect: RequestHandlerFactory =
        serverContext =>
          new RequestHandler(serverContext) {
            override def handle: PartialHandler[Http] = {
              case request @ Get on Root / "hello" =>
                Callback.successful(request.ok("Hello world!"))
            }
        }
    }
  }
}
// #example
