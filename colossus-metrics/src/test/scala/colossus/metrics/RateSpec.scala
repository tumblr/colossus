package colossus.metrics

import scala.concurrent.duration._


import akka.testkit.TestProbe

class RateSpec extends MetricIntegrationSpec {

  def rate() = new Rate("/foo")(new Collection(CollectorConfig(List(1.second, 1.minute))))

  "Rate" must {
    "increment in all intervals" in {
      val r = rate()
      r.hit()
      r.hit()
      r.tick(1.second)("/foo")(Map()) must equal(2)
      r.tick(1.minute)("/foo")(Map()) must equal(2)
    }

    "tick resets value for interval" in {
      val r = rate()
      r.hit()
      r.hit()
      r.tick(1.second)("/foo")(Map()) must equal(2)
      r.tick(1.minute)("/foo")(Map()) must equal(2)

      r.hit()
      r.tick(1.second)("/foo")(Map()) must equal(1)
      r.tick(1.minute)("/foo")(Map()) must equal(1)
    }

    "count" in {
      val r = rate()
      r.hit()
      r.hit()
      r.tick(1.second)("/foo/count")(Map()) must equal(2)

      r.hit()
      r.tick(1.second)("/foo/count")(Map()) must equal(3)
    }

    "count only updated on most granular tick" in {
      val r = rate()
      r.hit()
      r.hit()
      r.tick(1.minute)("/foo/count").isEmpty must equal(true)
      r.tick(1.second)("/foo/count")(Map()) must equal(2)
    }


  }


}

