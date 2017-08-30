import akka.actor.ActorSystem
import colossus.IOSystem
import colossus.core.ServerContext
import colossus.protocols.http.Http
import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.UrlParsing._
import colossus.protocols.http.{HttpServer, Initializer, RequestHandler}
import colossus.service.GenRequestHandler.PartialHandler
import colossus.service.Callback.Implicits._

// #example
object TestkitExample extends App {
  implicit val actorSystem = ActorSystem()
  implicit val ioSystem    = IOSystem()

  HttpServer.start("example-server", 9000) { initContext =>
    new Initializer(initContext) {
      override def onConnect = serverContext => new MyHandler(serverContext)
    }
  }
}

class MyHandler(context: ServerContext) extends RequestHandler(context) {
  override def handle: PartialHandler[Http] = {
    case request @ Get on Root / "foo" => request.ok("bar")
  }
}
// #example
