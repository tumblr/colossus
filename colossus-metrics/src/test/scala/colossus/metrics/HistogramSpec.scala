package colossus.metrics

import MetricAddress.Root
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

  "Histogram" must {
    "simple collection" in {
      val h = new BaseHistogram(Histogram.generateBucketRanges(10, 10))
      (0 to 10).foreach{h.add}
      h.min must equal (0)
      h.max must equal (10)
      h.count must equal (11)
      val percs = Seq(0, 0.25, 0.5, 0.75, 0.99, 1.0)
      h.snapshot.percentiles(percs) must equal (percs.zip(Seq(0, 2, 5, 8, 9, 10)).toMap)
    }


    "generate snapshot" in {
      val h = new BaseHistogram(Histogram.generateBucketRanges(10, 10))
      (0 to 10).foreach{h.add}
      val percs = List(0, 0.25, 0.5, 0.75, 0.99, 1.0)
      val values = Seq(0, 2, 5, 8, 9, 10)

      //h.snapshot(percs) must equal (Snapshot(0, 10, 11, percs.zip(values).toMap))
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



}
