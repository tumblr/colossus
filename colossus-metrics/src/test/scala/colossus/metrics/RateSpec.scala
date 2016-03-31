package colossus.metrics

import scala.concurrent.duration._


import akka.testkit.TestProbe

class RateSpec extends MetricIntegrationSpec {

  implicit val c = new Collection(CollectorConfig(List(1.second, 1.minute)))
  def rate() = new DefaultRate("/foo", false)

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

    "not return any metrics when never hit" in {
      rate().tick(1.second) must equal(Map())
    }

    "prune empty values" in {
      val r = new DefaultRate("/foo", true)
      r.hit(Map("a" -> "b"))
      r.hit(Map("b" -> "c"))
      r.hit(Map("b" -> "c"))
      val s = r.tick(1.second)
      s("foo").size must equal(2)
      s("foo/count").size must equal(2)
      r.hit(Map("a" -> "b"))
      val s2 = r.tick(1.second)
      s2("foo").size must equal(1)
      //counts don't get pruned since they're never 0...maybe this needs some thought
      //s2("foo/count").size must equal(1)
      s2("foo")(Map("a" -> "b")) must equal(1)
    }
  }
}

