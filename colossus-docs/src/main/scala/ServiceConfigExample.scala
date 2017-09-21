import akka.actor.ActorSystem
import colossus.IOSystem
import colossus.parsing.DataSize._
import colossus.protocols.http.Http
import colossus.protocols.http.HttpMethod._
import colossus.protocols.http.UrlParsing._
import colossus.protocols.http.{HttpServer, Initializer, RequestHandler}
import colossus.service._
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
    maxRequestSize = 10.MB,
    errorConfig = ErrorConfig(
      doNotLog = Set.empty[String],
      logOnlyName = Set("DroppedReplyException")
    )
  )
  // #example

  HttpServer.start("example-server", 9000) { initContext =>
    new Initializer(initContext) {
      // #example1
      override def onConnect = serverContext => new RequestHandler(serverContext, serviceConfig) {
        // #example1
        override def handle: PartialHandler[Http] = {
          case request @ Get on Root => Callback.successful(request.ok("Hello world!"))
        }

        // #example2
        override def requestLogFormat: Option[RequestFormatter[Request]] = {
          val customFormatter = new ConfigurableRequestFormatter[Request](serviceConfig.errorConfig) {
            override def format(request: Request, error: Throwable): Option[String] = {
              Some(s"$request failed with ${error.getClass.getSimpleName}")
            }
          }
          Some(customFormatter)
        }
        // #example2
      }
    }
  }
}
