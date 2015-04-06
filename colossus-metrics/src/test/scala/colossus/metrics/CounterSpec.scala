package colossus.metrics

import scala.concurrent.duration._

class CounterSpec extends MetricIntegrationSpec {

  "Basic Counter" must {
    "increment" in {
      val c = new BasicCounter(CounterParams("/foo"))
      c.increment()
      c.value().get must equal(1)

    }

    "decrement" in {
      val c = new BasicCounter(CounterParams("/foo"))
      c.increment()
      c.decrement()
      c.value().get must equal(0)
    }

    "delta" in {
      val c = new BasicCounter(CounterParams("/foo"))
      c.delta(5)
      c.delta(-1)
      c.value().get must equal(4)
    }   

    "correctly handle tags" in {
      val c = new BasicCounter(CounterParams("/foo"))
      c.increment(Map("a" -> "a"))
      c.delta(2, Map("a" -> "b"))
      c.value(Map("a" -> "a")).get must equal(1)
      c.value(Map("a" -> "b")).get must equal(2)
    }

    
  }


  "Shared counter" must {
    "report the correct event" in {
      import akka.testkit.TestProbe
      import colossus.metrics.testkit.TestSharedCollection
      import EventLocality._
      val probe = TestProbe()
      val collection = new TestSharedCollection(probe)
      val counter: Shared[Counter] = collection.getOrAdd(Counter("/foo"))
      counter.delta(2, Map("a" -> "aa"))
      probe.expectMsg(10.seconds, Counter.Delta("/foo", 2, Map("a" -> "aa")))
    }
  }

}
