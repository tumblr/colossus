package colossus.metrics

import scala.concurrent.duration._

import MetricValues._

import akka.testkit.TestProbe
import colossus.metrics.testkit.TestSharedCollection

import EventLocality._

class RateSpec extends MetricIntegrationSpec {

  "Basic Rate" must {
    "tick" in {
      val r = new BasicRate
      r.hit()
      r.hit()
      r.hit()
      r.value must equal(0)
      r.tick()
      r.value must equal(3)
    }

  }

  "ConcreteRate" must {
    "tick for tags" in {
      val params = Rate("/foo")
      val r = new ConcreteRate(params, CollectorConfig(List(1.second)))
      r.hit(Map("foo" -> "a"))
      r.hit(Map("foo" -> "a"))
      r.hit(Map("foo" -> "b"))
      val m1 = r.metrics(CollectionContext(Map(), 1.second))
      m1("/foo")(Map("foo" -> "a")) must equal (SumValue(0L))
      m1("/foo/count")(Map("foo" -> "a")) must equal (SumValue(2L))
      r.tick(1.second)
      val m2 = r.metrics(CollectionContext(Map(), 1.second))
      m2("/foo")(Map("foo" -> "a")) must equal (SumValue(2L))
    }


    "report the correct intervals" in {
      val params = Rate("/foo")
      val r = new ConcreteRate(params, CollectorConfig(List(1.second, 1.minute)))
      r.hit()
      r.hit()
      r.hit()

      def check(interval: FiniteDuration, expected: Long) {
        val m = r.metrics(CollectionContext(Map(), interval))
        m("/foo")(Map()) must equal (SumValue(expected))

      }

      r.tick(1.second)
      check(1.second, 3)
      check(1.minute, 0)

      r.tick(1.minute)
      check(1.second, 3)
      check(1.minute, 3)

      r.tick(1.second)
      check(1.second, 0)
      check(1.minute, 3)
    }

    "prune empty values" in {
      val r = new ConcreteRate(Rate("/foo", true), CollectorConfig(List(1.second, 1.minute)))
      r.hit(Map("foo" -> "a"))
      r.hit(Map("foo" -> "b"))
      r.tick(1.second)
      r.metrics(CollectionContext(Map(), 1.second))("/foo").contains(Map("foo" -> "a")) must equal(true)
      r.metrics(CollectionContext(Map(), 1.second))("/foo").contains(Map("foo" -> "b")) must equal(true)
      r.hit(Map("foo" -> "a"))
      r.tick(1.second)
      r.metrics(CollectionContext(Map(), 1.second))("/foo").contains(Map("foo" -> "a")) must equal(true)
      r.metrics(CollectionContext(Map(), 1.second))("/foo").contains(Map("foo" -> "b")) must equal(false)
    }


  }

  "Shared Rate" must {
    "send correct event" in {
      val probe = TestProbe()
      val collection = new TestSharedCollection(probe)
      val rate: Shared[Rate] = collection.getOrAdd(Rate("/foo"))
      rate.hit(tags = Map("a" -> "aa"))
      probe.expectMsg(10.seconds, Rate.Hit("/foo", Map("a" -> "aa"), 1))
    }
  }

}

