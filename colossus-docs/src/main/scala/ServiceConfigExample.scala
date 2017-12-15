import akka.actor.ActorSystem
import colossus.core.IOSystem
import colossus.util.DataSize._
import colossus.protocols.http.Http
import colossus.protocols.http.HttpMethod._
import colossus.protocols.http.UrlParsing._
import colossus.protocols.http.{HttpServer, Initializer, RequestHandler}
import colossus.service._
import colossus.service.GenRequestHandler.PartialHandler

import scala.concurrent.duration._

object ServiceConfigExample {
  implicit val actorSystem = ActorSystem()
  implicit val ioSystem    = IOSystem()

  // #example
  val serviceConfig = ServiceConfig(
    requestTimeout = 1.second,
    requestBufferSize = 100,
    logErrors = true,
    requestMetrics = true,
    maxRequestSize = 10.MB
  )
  // #example

  HttpServer.start("example-server", 9000) { initContext =>
    new Initializer(initContext) {
      // #example1
      override def onConnect: RequestHandlerFactory =
        serverContext =>
          new RequestHandler(serverContext, serviceConfig) {
            // #example1
            override def handle: PartialHandler[Http] = {
              case request @ Get on Root => Callback.successful(request.ok("Hello world!"))
            }

            // #example2
            override def requestLogFormat: RequestFormatter[Request] = new RequestFormatter[Request] {
              override def format(request: Option[Request], error: Throwable): String = {
                s"$request failed with ${error.getClass.getSimpleName}"
              }

              override def formatterOption(error: Throwable): RequestFormatType = RequestFormatType.LogWithStackTrace
            }
            // #example2
        }
    }
  }
}
