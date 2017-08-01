import akka.actor.ActorSystem
import colossus.IOSystem
import colossus.metrics.{MetricSystem, Rate}
import colossus.protocols.http.Http
import colossus.protocols.http.HttpMethod._
import colossus.protocols.http.UrlParsing._
import colossus.protocols.http.server.{HttpServer, Initializer, RequestHandler}
import colossus.service.Callback
import colossus.service.GenRequestHandler.PartialHandler

object MetricTickExample extends App {

  // #example
  implicit val actorSystem = ActorSystem()

  val metricSystem = MetricSystem("badurl")

  implicit val ioSystem = IOSystem("badurl", None, metricSystem)

  implicit val metricNamespace = ioSystem.metrics

  val badUrl = Rate("badurl")

  HttpServer.start("example-server", 9000) {
    new Initializer(_) {
      override def onConnect = new RequestHandler(_) {
        override def handle: PartialHandler[Http] = {
          case request @ Get on Root / "url" =>
            //bad url, metric tick
            if(!request.head.parameters.contains("myurl")) {
              badUrl.hit()
            }
            Callback.successful(request.ok("received response"))
        }
      }
    }
  }
  // #example
}
