import akka.actor.ActorSystem
import colossus.IOSystem
import colossus.parsing.DataSize._
import colossus.protocols.http.Http
import colossus.protocols.http.HttpMethod._
import colossus.protocols.http.UrlParsing._
import colossus.protocols.http.server.{HttpServer, Initializer, RequestHandler}
import colossus.service.{Callback, ServiceConfig}
import colossus.service.GenRequestHandler.PartialHandler

import scala.concurrent.duration._

object ServiceConfigExample {
  implicit val actorSystem = ActorSystem()
  implicit val ioSystem = IOSystem()

  // #example
  val serviceConfig = ServiceConfig(
    requestTimeout = 1.second,
    requestBufferSize = 100,
    logErrors = true,
    requestMetrics = true,
    maxRequestSize = 10.MB
  )
  // #example

  HttpServer.start("example-server", 9000) {
    new Initializer(_) {
      // #example1
      override def onConnect = new RequestHandler(_, serviceConfig) {
        // #example1
        override def handle: PartialHandler[Http] = {
          case request @ Get on Root => Callback.successful(request.ok("Hello world!"))
        }
      }
    }
  }
}
