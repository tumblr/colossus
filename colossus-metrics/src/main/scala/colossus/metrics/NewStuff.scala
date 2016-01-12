package colossus.metrics

import java.util.concurrent.ConcurrentHashMap

import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.duration._

object NewStuff {

  //these override the package-level ones
  type ValueMap = Map[TagMap, Long]
  type MetricMap = Map[MetricAddress, ValueMap]

trait Collector {
  def address: MetricAddress
  def tick(interval: FiniteDuration): MetricMap
}

class CollectionMap[T] {

  private val map = new ConcurrentHashMap[T, AtomicLong]

  def increment(tags: T, num: Long = 1) {
    Option(map.get(tags)) match {
      case Some(got) => got.addAndGet(num)
      case None => {
        map.putIfAbsent(tags, new AtomicLong(0))
        map.get(tags).addAndGet(num)
      }
    }
  }

  def snapshot(pruneEmpty: Boolean, reset: Boolean): Map[T, Long] = {
    val keys = map.keys
    var build = Map[T, Long]()
    while (keys.hasMoreElements) {
      val key = keys.nextElement
      val value: Long = if (reset) {
        val v = map.get(key).getAndSet(0L)
        // notice there's a race condition since between these two lines another
        // increment could occur and subsequently be lost, but it's an
        // acceptable loss
        if (pruneEmpty && v == 0) {
          map.remove(key, 0L)
        }
        v
      } else {
        map.get(key).get
      }
      build = build + (key -> value)
    }

    build
  }

}


class Rate(val address: MetricAddress, config: CollectorConfig) extends Collector {

  val maps = config.intervals.map{i => (i, new CollectionMap[TagMap])}.toMap

  def hit(tags: TagMap = TagMap.Empty, num: Long = 1) {
    maps.foreach{ case (_, map) => map.increment(tags) }
  }

  def tick(interval: FiniteDuration): MetricMap  = {
    Map(address -> maps(interval).snapshot(false, true))
  }
    

}

case class BucketValue(value: Int, count: Int)
case class Snapshot(min: Int, max: Int, count: Int, bucketValues: Vector[BucketValue]) {

  def percentiles(percs: Seq[Double]): Map[Double, Int] =  {
    def p(num: Int, index: Int, build: Seq[Int], remain: Seq[Double]): Seq[Int] = remain.headOption match {
      case None => build
      case Some(perc) => {
        if (perc <= 0.0 || count == 0) {
          p(num, index, build :+ 0, remain.tail)
        } else if (perc >= 1.0) {
          p(num, index, build :+ max, remain.tail)          
        } else {
          if (index < bucketValues.size - 1 && num < count * perc) {
            p(num + bucketValues(index).count, index + 1, build, remain)
          } else {
            p(num, index, build :+ bucketValues(index).value, remain.tail)
          }
        }
      }
    }
    val sorted = percs.sortWith{_ < _}
    sorted.zip(p(0, 0, Seq(), sorted)).toMap
  }

  def percentile(perc: Double): Int = percentiles(Seq(perc))(perc)

  def metrics(address: MetricAddress, tags: TagMap, percs: Seq[Double]): MetricMap = {
    val pvalues = percentiles(percs)
    Map (
      (address / "min") -> Map(tags -> min),
      (address / "max") -> Map(tags -> max),
      (address / "count") -> Map(tags -> count),
      address -> pvalues.map{case (p, v) => tags + ("percentile" -> p.toString) -> v.toLong}
    )
  }

}
object Snapshot {


}

/**
 * This is the actual histogram data structure.  It knows nothing of tags or metrics
 */
class BaseHistogram(val bucketList: BucketList = Histogram.defaultBucketRanges) {
  require (ranges.size > 1, "histogram must have at least 2 buckets")

  private lazy val ranges = bucketList.buckets

  private val infinity   = ranges.last
  private val mBuckets = Vector.fill(ranges.size)(new AtomicLong(0))

  private val mMax = new AtomicLong(0)
  private val mMin = new AtomicLong(infinity)
  private val mCount = new AtomicLong(0)

  def min = if (count > 0) mMin.get else 0
  def max = mMax.get
  def count = mCount.get
  def buckets = mBuckets

  def bucketFor(value: Int) = {
    def s(index: Int, n: Int): Int = if (ranges(index) > value) {
      s(index - n, math.max(1, n / 2))
    } else if (ranges(index + 1) <= value) {
      s(index + n, math.max(1, n / 2))
    } else {
      index
    }
    if (value >= infinity) {
      mBuckets.size - 1
    } else {
      s(mBuckets.size / 2, mBuckets.size / 2)
    }
  }

  def add(value: Int) {
    require(value >= 0, "value cannot be negative")
    mCount.incrementAndGet
    def compAndSet(l: AtomicLong, newVal: Long, c: (Long, Long) => Boolean) {
      val old = l.get
      if (c(old, newVal)) {
        var tries = 3
        while (!l.compareAndSet(old, newVal) && tries > 0) {
          tries -= 1
        }
      }
    }
    compAndSet(mMax, value, _ < _)
    compAndSet(mMin, value, _ > _)
    mBuckets(bucketFor(value)).incrementAndGet
  }


  def snapshot = {

    val smax = mMax.getAndSet(0)
    val smin = mMin.getAndSet(infinity)
    val scount = mCount.getAndSet(0)
    var values = Vector[BucketValue]()
    var index = 0
    while (index < mBuckets.size) {
      val v = mBuckets(index).getAndSet(0)
      if (v > 0) {
        values = values :+ BucketValue(value = ranges(index), count = v.toInt)
      }
    }    
    Snapshot(smin.toInt, smax.toInt, scount.toInt, values)
  }

}

class Histogram(val address: MetricAddress, percentiles: Seq[Double], config: CollectorConfig) extends Collector {

  val tagHists: Map[FiniteDuration, ConcurrentHashMap[TagMap, BaseHistogram]] = config.intervals.map{i => 
    val m = new ConcurrentHashMap[TagMap, BaseHistogram]
    (i -> m)
  }.toMap

  def add(value: Int, tags: TagMap = TagMap.Empty) {
    tagHists.foreach{ case (_, taghists) =>
      Option(taghists.get(tags)) match {
        case Some(got) => got.add(value)
        case None => {
          taghists.putIfAbsent(tags, new BaseHistogram)
          //TODO: possible race condition if removed between these lines
          taghists.get(tags).add(value)
        }
      }
    }
  }

  def tick(interval: FiniteDuration): MetricMap = {
    val taghist = tagHists(interval)
    val keys = taghist.keys
    var build: MetricMap = Map()
    while (keys.hasMoreElements) {
      val key = keys.nextElement
      val snap = taghist.get(key).snapshot
      build = build ++ snap.metrics(address, TagMap.Empty, percentiles)
    }
    build
  }

}


class Collection {

  val collectors = new ConcurrentHashMap[MetricAddress, Collector]

  def add(collector: Collector) {
    collectors.put(collector.address, collector)
  }

  def tick(interval: FiniteDuration): MetricMap = {
    val keys = collectors.keys
    var build: MetricMap = Map()
    while (keys.hasMoreElements) {
      val key = keys.nextElement
      Option(collectors.get(key)) foreach { c =>
        build = build ++ c.tick(interval)
      }
    }
    build
  }

  

}



}
