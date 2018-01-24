import colossus.metrics.{Collection, MetricAddress, MetricContext}
import colossus.protocols.http.{HttpRequest, HttpResponse}
import org.scalatest.WordSpec

import scala.concurrent.duration._

class SimpleMetricExampleSpec extends WordSpec {

  "Simple metric controller" must {
    // #example
    "say hello" in {
      implicit val metricContext: MetricContext = MetricContext("/", Collection.withReferenceConf(Seq(1.minute)))

      val controller = new SimpleMetricController()

      val response = controller.sayHello(HttpRequest.get("/hello?color=red"))

      assert(response == HttpResponse.ok("Hello!"))

      val metrics = metricContext.collection.tick(1.minute)

      assert(metrics(MetricAddress.Root / "hellos") === Map(Map("color" -> "red") -> 1))
    }
    // #example
  }

}
