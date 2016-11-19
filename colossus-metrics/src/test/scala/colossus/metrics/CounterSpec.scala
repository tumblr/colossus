package colossus.metrics

import scala.concurrent.duration._

class CounterSpec extends MetricIntegrationSpec {

  def counter = new DefaultCounter("/foo")

  "Counter" must {
    "increment" in {
      val c = counter
      c.get() must equal(0)
      c.increment()
      c.get() must equal(1)

    }

    "decrement" in {
      val c = counter
      c.increment()
      c.get() must equal(1)
      c.decrement()
      c.get() must equal(0)
    }

    "set" in {
      val c = counter
      c.set(value = 3456)
      c.get() must equal(3456)
    }

    "correctly handle tags" in {
      val c = counter
      c.set(tags = Map("a" -> "a"), 123)
      c.increment(tags = Map("a" -> "b"))
      c.increment(tags = Map("a" -> "b"))
      c.get(Map("a" -> "a")) must equal(123)
      c.get(Map("a" -> "b")) must equal(2)
    }

    "return no metrics when not used yet" in {
      counter.tick(1.second) must equal(Map())
    }

    "have correct address" in {
      implicit val ns = MetricContext("/foo", Collection.withReferenceConf(Seq(1.second))) / "bar"
      val c = Counter("/baz")
      c.address must equal(MetricAddress("/foo/bar/baz"))

    }


  }

}
