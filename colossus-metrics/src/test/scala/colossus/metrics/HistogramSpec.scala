package colossus.metrics

import org.scalatest._

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

    /*

    "generate snapshot" in {
      val h = new BaseHistogram(Histogram.generateBucketRanges(10, 10))
      (0 to 10).foreach{h.add}
      val percs = Seq(0, 0.25, 0.5, 0.75, 0.99, 1.0)
      val values = Seq(0, 2, 5, 8, 9, 10)

      h.snapshot(percs) must equal (Snapshot(Root, 0, 10, 11, percs.zip(values).toMap))
    }
    */


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
  /*

  "tagged histogram" must {
    "combine values with same tags" in {
      val h = new TaggedHistogram(Root)
      val m = Map("foo" -> "bar")
      h.add(m, 4)
      h.add(m, 400)
      val percs = Seq(0.25, 0.5, 1.0)
      h(m).percentiles(percs) must equal (percs.zip(Seq(4, 4, 400)).toMap)
    }

    "seperate values with different tag values" in {
      val h = new TaggedHistogram(Root)
      h.add(Map("foo" -> "a"), 3)
      h.add(Map("foo" -> "b"), 30000)
      h(Map("foo" -> "a")).max must equal (3)
      h(Map("foo" -> "b")).max must equal (30000)
    }

    "create new histogram on non-existant tagmap" in {
      val h = new TaggedHistogram(Root)
      h(Map("not" -> "exists")).count must equal (0)
    }

    "produce raw stats" in {
      val h = new TaggedHistogram(Root)
      val exp1 = Histogram()
      val exp2 = Histogram()
      val percs = Seq(0.75, 0.99)
      (0 to 100).foreach{i =>
        h.add(Map("foo" -> "bar"), i)
        exp1.add(i)
      }
      (100 to 200).foreach{i =>
        h.add(Map("foo" -> "baz"), i)
        exp2.add(i)
      }
      val perc: Metric = Metric(Root, 
        exp1.percentiles(percs).map{ case (perc, value) => 
          Map("foo" -> "bar", "percentile" -> perc.toString) -> value.toLong
        } ++ exp2.percentiles(percs).map{ case (perc, value) => 
          Map("foo" -> "baz", "percentile" -> perc.toString) -> value.toLong
        }
      )
      val min = Metric(Root / "min", Map(
        Map("foo" -> "bar") -> 0L,
        Map("foo" -> "baz") -> 100L
      ))
      val max = Metric(Root / "max", Map(
        Map("foo" -> "bar") -> 100L,
        Map("foo" -> "baz") -> 200L
      ))
      val count = Metric(Root / "count", Map(
        Map("foo" -> "bar") -> 101L,
        Map("foo" -> "baz") -> 101L
      ))
      val expected = MetricMap(perc, min,max, count)
      val actual = h.metrics(percs)

      if (actual != expected) {
        println("EXPECTED")
        expected.foreach{case (address, values) => println(address.toString + ": " + values.lineString)}
        println("ACTUAL")
        actual.foreach{case (address, values) => println(address.toString + ": " + values.lineString)}
      }
      actual must equal (expected)
    }


  }
  */
      
}
