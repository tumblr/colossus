package colossus.metrics

import org.scalatest.{MustMatchers, WordSpec}
import scala.concurrent.duration._

class CollectionSpec extends WordSpec with MustMatchers{

  "Collection" must {

    implicit val collection = new Collection(CollectorConfig(Seq(1.minute, 1.second)))

    "getOrAdd should add a new Collector" in {
      //note, this constructor implicitly adds its constructed object into this collection
      val foo = MetricAddress("foo")
      val rate = Rate(foo, false, true)

      val x = collection.collectors.get(foo)
      x mustBe a[Rate]
      //this is a reference check, since we are not overriding .equals yet
      x.asInstanceOf[Rate] mustBe rate
    }

    "getOrAdd should return an existing Collector" in {
      //note, this constructor implicitly adds its constructed object into this collection
      val bar = MetricAddress("bar")

      val rate = Rate(bar, false, true)
      val rate2 = Rate(bar, false, true)
      val x = collection.collectors.get(bar)
      x mustBe a[Rate]
      //reference check again, this should be pointing at the first, since the second should have never got added
      x.asInstanceOf[Rate] mustBe rate
    }

    "getOrAdd should throw if a metric with the same address, but different type is used" in {
      val bar = MetricAddress("baz")
      val rate = Rate(bar, false, true)
      //TODO FIX ME I need to be a DuplicateMetricException
      intercept[ClassCastException]{
        Counter(bar, false)
      }
    }
  }

}
