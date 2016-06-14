package colossus.metrics

import org.scalatest.{MustMatchers, WordSpec}
import scala.concurrent.duration._

class CollectionSpec extends WordSpec with MustMatchers{

  "Collection" must {

    val collection = Collection.withReferenceConf(Seq(1.minute, 1.second))

    implicit val namespace = MetricContext("/", collection)

    "getOrAdd should add a new Collector" in {
      //note, this constructor implicitly adds its constructed object into this collection
      val foo = MetricAddress("foo")
      val rate = Rate(foo, false, true)

      val x = namespace.collection.collectors.get(foo)
      x.collector mustBe a[Rate]
      //this is a reference check, since we are not overriding .equals yet
      x.collector.asInstanceOf[Rate] mustBe rate
    }

    "getOrAdd should return an existing Collector" in {
      //note, this constructor implicitly adds its constructed object into this collection
      val bar = MetricAddress("bar")

      val rate = Rate(bar, false, true)
      val rate2 = Rate(bar, false, true)
      val x = namespace.collection.collectors.get(bar)
      x.collector mustBe a[Rate]
      //reference check again, this should be pointing at the first, since the second should have never got added
      x.collector.asInstanceOf[Rate] mustBe rate
    }

    "getOrAdd should throw if a metric with the same address, but different type is used" in {
      val bar = MetricAddress("baz")
      val rate = Rate(bar, false, true)
      intercept[DuplicateMetricException]{
        Counter(bar, false)
      }
    }

    "aggregate tags" in {
      val subNamespace = namespace / "foo" * ("a" -> "b")
      val bar = MetricAddress("baz")
      val rate = Rate(bar, false, true)(subNamespace)
      rate.hit()
      collection.tick(1.minute) mustBe Map(
        MetricAddress("/foo/baz") -> Map(Map("a" -> "b") -> 1),
        MetricAddress("/foo/baz/count") -> Map()
      )
    }
  }

}
