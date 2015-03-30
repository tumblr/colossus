package colossus.metrics

import org.scalatest._

import MetricAddress.Root

class HistogramSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {
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
      val h = new BaseHistogram(Histogram.generateBucketRanges(10, 10))
      (0 to 10).foreach{h.add}
      h.min must equal (0)
      h.max must equal (10)
      h.count must equal (11)
      val percs = Seq(0, 0.25, 0.5, 0.75, 0.99, 1.0)
      h.percentiles(percs) must equal (percs.zip(Seq(0, 2, 5, 8, 9, 10)).toMap)
    }


    "generate snapshot" in {
      val h = new BaseHistogram(Histogram.generateBucketRanges(10, 10))
      (0 to 10).foreach{h.add}
      val percs = List(0, 0.25, 0.5, 0.75, 0.99, 1.0)
      val values = Seq(0, 2, 5, 8, 9, 10)

      h.snapshot(percs) must equal (Snapshot(0, 10, 11, percs.zip(values).toMap))
    }


    "bucketFor" in {
      val h = new BaseHistogram

      h.bucketFor(5) must equal (5)
    }

    "large distribution" in {
      val h = new BaseHistogram
      (0 to 50000).foreach{h.add}
      val zipped = h.bucketList.buckets.zip(h.buckets)
      (0 until zipped.size - 1).foreach{i =>
        val (cBucket, cCount) = zipped(i)
        val (nBucket, nCount) = zipped(i + 1)
        if (nBucket < 50000) {
          (nBucket - cBucket) must equal (cCount)
        }
      }
    }
  }

  "tagged histogram" must {
    "combine values with same tags" in {
      val h = new TaggedHistogram(Histogram.generateBucketRanges(10))
      val m = Map("foo" -> "bar")
      h.add(4, m)
      h.add(400, m)
      val percs = Seq(0.25, 0.5, 1.0)
      h(m).percentiles(percs) must equal (percs.zip(Seq(4, 4, 400)).toMap)
    }

    "seperate values with different tag values" in {
      val h = new TaggedHistogram(Histogram.generateBucketRanges(10))
      h.add(3, Map("foo" -> "a"))
      h.add(30000, Map("foo" -> "b"))
      h(Map("foo" -> "a")).max must equal (3)
      h(Map("foo" -> "b")).max must equal (30000)
    }

    "produce raw stats" in {
      val h = new TaggedHistogram(Histogram.generateBucketRanges(10))
      val exp1 = new BaseHistogram(Histogram.generateBucketRanges(10))
      val exp2 = new BaseHistogram(Histogram.generateBucketRanges(10))
      val percs = List(0.75, 0.99)
      (0 to 100).foreach{i =>
        h.add(i, Map("foo" -> "bar"))
        exp1.add(i)
      }
      (100 to 200).foreach{i =>
        h.add(i, Map("foo" -> "baz"))
        exp2.add(i)
      }
      val expected: Map[TagMap, Snapshot] = Map(
        Map("foo" -> "bar") -> Snapshot(0, 100, 101, exp1.percentiles(percs)),
        Map("foo" -> "baz") -> Snapshot(100, 200, 101, exp2.percentiles(percs))
      )
      val actual = h.snapshots(percs)

      actual must equal (expected)
    }


  }

  "PeriodicHistogram" must {
    "properly tick for intervals" in {
      //val params = HistogramParams(

    }

  }
      
}
