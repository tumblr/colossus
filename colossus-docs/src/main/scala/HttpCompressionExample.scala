import akka.actor.ActorSystem
import colossus.IOSystem
import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.UrlParsing.{Root, on}
import colossus.protocols.http.filters._
import colossus.protocols.http.{Http, HttpServer, Initializer, RequestHandler}
import colossus.service.Callback.Implicits._
import colossus.service.Filter
import colossus.service.GenRequestHandler.PartialHandler

object HttpCompressionExample extends App {

  implicit val actorSystem = ActorSystem()
  implicit val ioSystem    = IOSystem()

  HttpServer.start("example-server", 9000) { initContext =>
    new Initializer(initContext) {
      override def onConnect: RequestHandlerFactory =
        serverContext =>
          new RequestHandler(serverContext) {
            override def handle: PartialHandler[Http] = {
              case request @ Get on Root => request.ok("Hello world!")
            }
            // Enable Gzip/Deflate
            override def filters: Seq[Filter[Http]] = new HttpCustomFilters.CompressionFilter() :: Nil
        }
    }
  }
}
