import akka.actor.ActorSystem
import colossus.IOSystem
import colossus.core.ServerContext
import colossus.metrics.{MetricNamespace, MetricSystem}
import colossus.metrics.collectors.Counter
import colossus.protocols.http.HttpMethod._
import colossus.protocols.http.UrlParsing._
import colossus.protocols.http.{Http, HttpRequest, HttpResponse, HttpServer, Initializer, RequestHandler}
import colossus.service.Callback.Implicits._
import colossus.service.GenRequestHandler.PartialHandler

object SimpleMetricExample extends App {

  implicit val actorSystem: ActorSystem = ActorSystem()

  implicit val ioSystem: IOSystem = IOSystem("iosystem", None, MetricSystem("badurl"))

  implicit val metricNamespace: MetricNamespace = ioSystem.metrics

  val controller = new SimpleMetricController()

  HttpServer.start("example-server", 9000) { initContext =>
    new Initializer(initContext) {
      override def onConnect: RequestHandlerFactory = { serverContext =>
        new SimpleMetricRequestHandler(serverContext, controller)
      }
    }
  }
}

class SimpleMetricRequestHandler(serverContext: ServerContext, controller: SimpleMetricController)
    extends RequestHandler(serverContext) {
  override protected def handle: PartialHandler[Http] = {
    case request @ Get on Root / "hello" => controller.sayHello(request)
  }
}

// #example
class SimpleMetricController()(implicit mn: MetricNamespace) {
  private val hellos = Counter("hellos")

  def sayHello(request: HttpRequest): HttpResponse = {
    val color = request.head.parameters.getFirst("color").getOrElse("NA")
    hellos.increment(Map("color" -> color))
    request.ok("Hello!")
  }
}
// #example
