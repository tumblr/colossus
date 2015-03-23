package colossus.metrics

import scala.concurrent.duration._

class RateSpec extends MetricIntegrationSpec {

  "Basic Rate" must {
    "tick" in {
      val r = new BasicRate(2.seconds)
      r.hit()
      r.hit()
      r.hit()
      r.value must equal(0)
      r.tick(1.second)
      r.value  must equal(0)
      r.tick(1.second)
      r.value must equal(3)

      r.tick(2.seconds)
      r.value must equal(0)
    }

  }

  "ConcreteRate" must {
    "tick for tags" in {
      val params = Rate("/foo", periods = List(1.second))
      val r = new ConcreteRate(params)
      r.hit(Map("foo" -> "a"))
      r.hit(Map("foo" -> "a"))
      r.hit(Map("foo" -> "b"))
      val m1 = r.metrics(CollectionContext(Map()))
      m1("/foo")(Map("foo" -> "a", "period" -> "1")) must equal (0L)
      m1("/foo/count")(Map("foo" -> "a")) must equal (2L)
      r.tick(1.second)
      val m2 = r.metrics(CollectionContext(Map()))
      m2("/foo")(Map("foo" -> "a", "period" -> "1")) must equal (2L)
    }
  }

}

