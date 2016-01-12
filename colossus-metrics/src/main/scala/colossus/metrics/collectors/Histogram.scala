package colossus.metrics

import akka.actor._

import scala.concurrent.duration._

trait Histogram extends EventCollector {
  def add(value: Int, tags: TagMap = TagMap.Empty)
}

case class BucketList(buckets: Vector[Int]) extends AnyVal
  
case class HistogramParams (
  address: MetricAddress, 
  bucketRanges: BucketList = Histogram.defaultBucketRanges, 
  percentiles: List[Double] = Histogram.defaultPercentiles,
  sampleRate: Double = 1.0,
  pruneEmpty: Boolean = false
) extends MetricParams[Histogram, HistogramParams] {
  def transformAddress(f: MetricAddress => MetricAddress) = copy(address = f(address))
}

/**
 * A Basic log-scale histogram, mainly designed to measure latency
 *
 * Each bucket handles an increasingly large range of values from 0 to MAX_INT.
 */
object Histogram {

  val NUM_BUCKETS = 100
  //note, the value at index i is the lower bound of that bucket
  //also, the last bucket is the infinity bucket and only exists to make calculations easier (beware (0 to N) is inclusive)
  val defaultBucketRanges: BucketList = generateBucketRanges(NUM_BUCKETS)
  val defaultPercentiles: List[Double] = List(0.75, 0.9, 0.99, 0.999, 0.9999)

  /**
   * generate some bucket ranges for a histogram.  inifnity is the lower bound
   * of the last bucket.  You can set this to a value lower than MAX_INT when
   * you're confident most values will be below it.  For example, if you're
   * measuring processing latency, and you're sure a request will never take
   * more than 1000ms, you can set infinity to this, which will greatly improve
   * accuracy for values below, but all values above will be lumped into a
   * single bucket
   */
  def generateBucketRanges(num: Int, infinity: Int = Int.MaxValue) = {
    val mult = math.log(infinity) //log is actually ln
    val buckets = (0 to num).map{i =>
      if (i == 0) {
        0
      } else if (i == num) {
        infinity
      }else {
        math.max(i, math.pow(math.E ,mult * i / num).toInt)
      }
    }.toVector
    BucketList(buckets)
  }

  def apply(
    address: MetricAddress, 
    bucketRanges: BucketList = Histogram.defaultBucketRanges, 
    percentiles: List[Double] = Histogram.defaultPercentiles,
    sampleRate: Double = 1.0,
    pruneEmpty: Boolean = false
  ) : HistogramParams = HistogramParams(address, bucketRanges, percentiles, sampleRate, pruneEmpty)

  implicit object HistogramGenerator extends Generator[Histogram, HistogramParams] {
    def local(params: HistogramParams, config: CollectorConfig) = new PeriodicHistogram(params, config)
    def shared(params: HistogramParams, config: CollectorConfig)(implicit actor: ActorRef) = new SharedHistogram(params, actor)
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
