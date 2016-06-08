package colossus.metrics

import org.scalatest.{MustMatchers, WordSpec}
import scala.concurrent.duration._

class MetricNamespaceSpec extends WordSpec with MustMatchers{

  "MetricNamespace" must {
    "allow for subspaces to create Metrics with the same name" in {

      import scala.collection.JavaConversions._
      //not using implicits here, just to illustrate the point.
      val namespace = MetricContext("/", Collection.withReferenceConf(Seq(1.minute, 1.second)))
      val subName = namespace / "bar"

      //create 2 rates in the parent namespace
      Rate("bar", false, true)(namespace)
      Rate("baz", false, true)(namespace)
      //create 2 metrics in the sub namespace, with the same name
      Counter("bar", true)(subName)
      Rate("baz", true)(subName)

      //all 4 should be registered
      namespace.collection.collectors must have size 4

      //dupes should not be registered
      Rate("bar", false, true)(namespace)
      namespace.collection.collectors must have size 4

      val bar = MetricAddress("/bar")
      val baz = MetricAddress("/baz")
      val barBaz = MetricAddress("/bar/baz")
      val barBar = MetricAddress("/bar/bar")

      val expected = Set(bar, baz, barBaz, barBar)
      namespace.collection.collectors.keySet().toSet mustBe expected

      //paranoid much?
      namespace.collection.collectors.get(bar).collector mustBe a[Rate]
      namespace.collection.collectors.get(baz).collector mustBe a[Rate]
      namespace.collection.collectors.get(barBar).collector mustBe a[Counter]
      namespace.collection.collectors.get(barBaz).collector mustBe a[Rate]
      //yes
    }

    "add tags" in {
      val namespace = MetricContext("/", Collection.withReferenceConf(Seq(1.minute, 1.second)))
      val subnameSpace: MetricContext = namespace / "foo" * ("a" -> "b") / "bar"

      subnameSpace.tags mustBe Map("a" -> "b")

    }
  }
}
