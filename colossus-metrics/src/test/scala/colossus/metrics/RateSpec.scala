package colossus.metrics

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

class RateSpec extends MetricIntegrationSpec {

  def rate() = new DefaultRate("/foo", false, List(1.second, 1.minute))

  "Rate" must {
    "increment in all intervals" in {
      val r = rate()
      r.hit()
      r.hit()
      r.tick(1.second)("/foo")(Map()) must equal(2)
      r.tick(1.minute)("/foo")(Map()) must equal(2)
    }

    "increment by amount" in {
      val r = rate()
      r.hit(amount = 23)
      r.value(1.second) mustBe 23
      r.tick(1.second)("/foo")(Map()) must equal(23)
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
      r.count() mustBe 2
      r.tick(1.second)
      r.count() mustBe 2
    }

    "get the current value for an interval" in {
      val r = rate()
      r.hit()
      r.hit()
      r.value(1.second) mustBe 2
      r.tick(1.second)
      r.value(1.second) mustBe 0
    }

    "count in metrics" in {
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
      val r = new DefaultRate("/foo", true, List(1.second, 1.minute))
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

    "have the right address" in {
      implicit val ns = MetricContext("/foo", Collection.withReferenceConf(Seq(1.second))) / "bar"
      val r = Rate("/baz")
      r.address must equal(MetricAddress("/foo/bar/baz"))

    }

    "correctly handle hits from multiple threads" in {
      val r = rate()
      val f = Future.sequence{(1 to 10000).map{_ =>
        Future { 
          (1 to 5).foreach(_ => r.hit()) 
        }
      }}
      Await.result(f, 1.second)
      r.tick(1.second)("/foo")(Map()) must equal(50000)
    }
  }
}

