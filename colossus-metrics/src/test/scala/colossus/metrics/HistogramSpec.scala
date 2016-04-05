package colossus.metrics

import scala.concurrent.duration._

class HistogramSpec extends MetricIntegrationSpec {
  "Bucket generator" must {
    "generate bucket ranges" in {
      Histogram.generateBucketRanges(10, 500) must equal (BucketList(Vector(0, 1, 3, 6, 12, 22, 41, 77, 144, 268, 500)))
    }
    "linear" in {
      Histogram.generateBucketRanges(10, 10) must equal (BucketList(Vector(0,1,2,3,4,5,6,7,8,9,10)))
    }
  }

  "BaseHistogram" must {
    "simple collection" in {
      val address = MetricAddress.Root / "latency"
      val tags = Map("route" -> "home")
      val h = new BaseHistogram(Histogram.generateBucketRanges(10, 10))
      (0 to 10).foreach{h.add}
      h.min must equal (0)
      h.max must equal (10)
      h.count must equal (11)
      val percentiles = Seq(0.0, 0.25, 0.5, 0.75, 0.99, 1.0)
      val snapshot = h.snapshot
      snapshot.percentiles(percentiles) must equal (percentiles.zip(Seq(0, 3, 6, 9, 10, 10)).toMap)

      val metrics = Map(
        address / "count" -> Map(tags -> 11),
        address -> Map(
          tags + ("label" -> "min") -> 0,
          tags + ("label" -> "max") -> 10,
          tags + ("label" -> "mean") -> 5,
          tags + ("label" -> "0.0") -> 0,
          tags + ("label" -> "0.25") -> 3,
          tags + ("label" -> "0.5") -> 6,
          tags + ("label" -> "0.75") -> 9,
          tags + ("label" -> "0.99") -> 10,
          tags + ("label" -> "1.0") -> 10
        )
      )
      snapshot.metrics(address, tags, percentiles) must equal (metrics)
    }

    "bucketFor" in {
      val h = new BaseHistogram

      h.bucketFor(5) must equal (5)
    }

    "return 0 for min when empty" in {
      (new BaseHistogram).snapshot.min must equal(0)
    }

    "return 0 for mean when empty" in {
      (new BaseHistogram).snapshot.mean must equal(0)
    }

    "handle possible race condition" in {
      val s = Snapshot(0,0 ,0, 1, Vector())
      s.percentiles(Seq(0.5, 1.0)) must equal(Map(0.5 -> 0, 1.0 -> 0))
    }

  }

  "Histogram" must {
    "get tags right" in {
      implicit val col = MetricContext("/", new Collection(CollectorConfig(List(1.second))))
      val addr = MetricAddress.Root / "hist"
      val h = Histogram(addr)
      h.add(10, Map("foo" -> "bar"))
      h.add(20, Map("foo" -> "baz"))
      h.add(20, Map("foo" -> "baz"))

      val m = h.tick(1.second)
      m(addr)(Map("foo" -> "bar", "label" -> "min")) must equal(10)
      m(addr)(Map("foo" -> "baz", "label" -> "min")) must equal(20)
      m(addr / "count")(Map("foo" -> "bar")) must equal(1)
      m(addr / "count")(Map("foo" -> "baz")) must equal(2)
    }

    "prune empty values" in {
      implicit val col = MetricContext("/", new Collection(CollectorConfig(List(1.second))))
      val addr = MetricAddress.Root / "hist"
      val h = Histogram(addr, pruneEmpty = true)
      h.add(10, Map("foo" -> "bar"))
      h.add(20, Map("foo" -> "baz"))
      h.add(20, Map("foo" -> "baz"))
      val m = h.tick(1.second)
      m(addr)(Map("foo" -> "bar", "label" -> "min")) must equal(10)
      m(addr)(Map("foo" -> "baz", "label" -> "min")) must equal(20)
      h.add(10, Map("foo" -> "bar"))
      val m2 = h.tick(1.second)
      m(addr)(Map("foo" -> "bar", "label" -> "min")) must equal(10)
      m2(addr).get(Map("foo" -> "baz", "label" -> "min")).isEmpty must equal(true)
    }
  }



}
