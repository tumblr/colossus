package colossus.metrics

class HistogramSpec extends MetricIntegrationSpec {
  "Bucket generator" must {
    "generate bucket ranges" in {
      Histogram.generateBucketRanges(10, 500) must equal (BucketList(Vector(0, 1, 3, 6, 12, 22, 41, 77, 144, 268, 500)))
    }
    "linear" in {
      Histogram.generateBucketRanges(10, 10) must equal (BucketList(Vector(0,1,2,3,4,5,6,7,8,9,10)))
    }
  }

  "Histogram" must {
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

  }



}
