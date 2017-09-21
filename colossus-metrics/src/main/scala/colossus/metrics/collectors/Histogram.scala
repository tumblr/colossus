package colossus.metrics.collectors

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ThreadLocalRandom

import colossus.metrics.{Collector, MetricAddress, MetricMap, MetricNamespace, TagMap, ValueMap}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import com.typesafe.config.Config

/**
  * Configuration object for how a histogram's buckets should be specified
  */
sealed trait BucketConfig
object BucketConfig {
  case class Manual(buckets: List[Int])                  extends BucketConfig
  case class LinearScale(numBuckets: Int, infinity: Int) extends BucketConfig
  case class LogScale(numBuckets: Int, infinity: Int)    extends BucketConfig

  def fromConfig(config: Config): BucketConfig = {
    def infinity: Int = config.getString("infinity").toUpperCase match {
      case "MAX" => Int.MaxValue
      case other => other.toInt
    }
    config.getString("type").trim.toUpperCase match {
      case "MANUAL" => Manual(config.getIntList("values").asScala.map { _.toInt }.toList)
      case "LINEARSCALE" =>
        LinearScale(
          config.getInt("num-buckets"),
          infinity
        )
      case "LOGSCALE" =>
        LogScale(
          config.getInt("num-buckets"),
          infinity
        )
      case other => throw new Exception(s"Unknown Histogram bucket scheme $other")
    }
  }

  def buckets(config: BucketConfig): BucketList = config match {
    case Manual(buckets) => BucketList(buckets.toVector)
    case LinearScale(num, inf) =>
      BucketList((0 to num).map { i =>
        i * (inf / num)
      }.toVector)
    case LogScale(num, inf) => Histogram.generateBucketRanges(num, inf)
  }
}

/**
  * Metrics Collector which measures the distribution of values.
  * A single Histogram instance divides valuess up by TagMaps and track each one independently
  * When they are collected and reported, all TagMaps will be reported under the same MetricAddress.
  */
trait Histogram extends Collector {

  /**
    * The percentiles that this Histogram should distribute its values.
    *
    * @return
    */
  def percentiles: Seq[Double]

  /**
    * How often to collect values.
    *
    * @return
    */
  def sampleRate: Double

  /**
    * Instruct the collector to not report any values for tag combinations which were previously empty.
    *
    * @return
    */
  def pruneEmpty: Boolean

  /**
    * The buckets to use to group histogram values
    */
  def buckets: BucketList

  /**
    * Add a new value to this histogram,
    *
    * @param value The value to add
    * @param tags The TagMap used to record this value
    */
  def add(value: Int, tags: TagMap = TagMap.Empty)

  /**
    * get the current percentile for a collection interval.
    *
    * @param collectionInterval The collection interval to get the percentile for.
    * @param percent The pecentile to get
    * @param tags The tags to get the percentile for
    */
  def percentile(collectionInterval: FiniteDuration, percent: Double, tags: TagMap = TagMap.Empty): Int

  /**
    * Get the total number hits for a set of tags in a collection interval
    */
  def count(collectionInterval: FiniteDuration, tags: TagMap = TagMap.Empty): Int

}

/**
  * A BucketList contains an ascending-sorted list of lower bounds to use as
  * buckets for a histogram.  A value added to a histogram will get added to the
  * first bucket whose lower bound is less then the value.
  */
case class BucketList(buckets: Vector[Int]) extends AnyVal

/**
  * A Basic log-scale histogram, mainly designed to measure latency
  *
  * Each bucket handles an increasingly large range of values from 0 to MAX_INT.
  */
object Histogram {

  private val DefaultConfigPath = "histogram"

  val NUM_BUCKETS = 100
  //note, the value at index i is the lower bound of that bucket
  //also, the last bucket is the infinity bucket and only exists to make calculations easier (beware (0 to N) is inclusive)
  val defaultBucketRanges: BucketList  = generateBucketRanges(NUM_BUCKETS)
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
  def generateBucketRanges(num: Int, infinity: Int = Int.MaxValue): BucketList = {
    val mult = math.log(infinity) //log is actually ln
    val buckets = (0 to num).map { i =>
      if (i == 0) {
        0
      } else if (i == num) {
        infinity
      } else {
        math.max(i, math.pow(math.E, mult * i / num).toInt)
      }
    }.toVector
    BucketList(buckets)
  }

  /**
    * Create a Histogram with the following address.  See the documentation for [[colossus.metrics.MetricSystem]]
    *
    * @param address The MetricAddress of this Histogram.  Note, this will be relative to the containing MetricSystem's metricAddress.
    * @param ns The namespace to which this Metric is relative.
    * @return
    */
  def apply(address: MetricAddress)(implicit ns: MetricNamespace): Histogram = {
    apply(address, DefaultConfigPath)
  }

  /**
    * Create a Histogram with the following address, whose definitions is contained the specified configPath.
    * See the documentation for [[colossus.metrics.MetricSystem]]
    *
    * @param address The MetricAddress of this Histogram.  Note, this will be relative to the containing MetricSystem's metricAddress.
    * @param configPath The path in the config that this histogram's configuration is located.  This is relative to the MetricSystem config
    *                   definition.
    * @param ns The namespace to which this Metric is relative.
    * @return
    */
  def apply(address: MetricAddress, configPath: String)(implicit ns: MetricNamespace): Histogram = {
    ns.getOrAdd(address) { (fullAddress, config) =>

      val params      = config.resolveConfig(fullAddress, DefaultConfigPath, configPath)
      val percentiles = params.getDoubleList("percentiles").asScala.map(_.toDouble)
      val sampleRate  = params.getDouble("sample-rate")
      val pruneEmpty  = params.getBoolean("prune-empty")
      val enabled     = params.getBoolean("enabled")
      val buckets     = BucketConfig.buckets(BucketConfig.fromConfig(params.getConfig("buckets")))
      createHistogram(fullAddress, percentiles, sampleRate, pruneEmpty, enabled, config.intervals, buckets)
    }
  }

  /**
    * @param address The MetricAddress of this Histogram.  Note, this will be relative to the containing MetricSystem's metricAddress.
    * @param percentiles The percentiles that this Histogram should distribute its values.
    * @param sampleRate How often to collect values.
    * @param pruneEmpty Instruct the collector to not report any values for tag combinations which were previously empty.
    * @param enabled If this Histogram will actually be collected and reported.
    * @param ns The namespace to which this Metric is relative.
    * @return
    */
  def apply(
      address: MetricAddress,
      percentiles: Seq[Double] = Histogram.defaultPercentiles,
      sampleRate: Double = 1.0,
      pruneEmpty: Boolean = false,
      enabled: Boolean = true,
      buckets: BucketList = Histogram.defaultBucketRanges
  )(implicit ns: MetricNamespace): Histogram = {
    ns.getOrAdd(address) { (fullAddress, config) =>
      createHistogram(fullAddress, percentiles, sampleRate, pruneEmpty, enabled, config.intervals, buckets)
    }
  }

  private def createHistogram(address: MetricAddress,
                              percentiles: Seq[Double],
                              sampleRate: Double,
                              pruneEmpty: Boolean,
                              enabled: Boolean,
                              intervals: Seq[FiniteDuration],
                              buckets: BucketList): Histogram = {
    if (enabled) {
      new DefaultHistogram(address, percentiles, sampleRate, pruneEmpty, buckets, intervals)
    } else {
      new NopHistogram(address)
    }
  }
}

/**
  * This is the actual histogram data structure.  It knows nothing of tags or metrics
  */
private[metrics] class BaseHistogram(val bucketList: BucketList = Histogram.defaultBucketRanges) {
  require(ranges.size > 1, "histogram must have at least 2 buckets")

  private lazy val ranges = bucketList.buckets

  private val infinity = ranges.last
  private val mBuckets = Vector.fill(ranges.size)(new AtomicLong(0))

  private val mMax   = new AtomicLong(0)
  private val mMin   = new AtomicLong(infinity)
  private val mCount = new AtomicLong(0)
  private val mTotal = new AtomicLong(0)

  def min: Long = if (count > 0) mMin.get else 0L

  def max: Long = mMax.get

  def count: Long = mCount.get

  def buckets: Seq[AtomicLong] = mBuckets

  def mean: Long = {
    val scount = count
    if (scount > 0) mTotal.get / scount else 0L
  }

  def bucketFor(value: Int): Int = {
    def s(index: Int, n: Int): Int =
      if (ranges(index) > value) {
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
    require(value >= 0, s"value $value cannot be negative")
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
    mTotal.getAndAdd(value)
  }

  def tick() {
    mMax.set(0)
    mCount.set(0)
    mMin.set(0)
    mMax.set(0)
    mTotal.set(0)
    mBuckets.foreach { _.set(0) }
  }

  def percentiles(percs: Seq[Double]): Map[Double, Int] = {
    // NOTE: Very rarely, a race condition can occur that will cause calculated tail percentiles of a histogram (> 99th percentile)
    // to report Int.MaxValue. The bug is due to a race condition that can happen when a value is added to the histogram at the same
    // time the percentiles are being calculated in another thread. What ends up happening is that the calculated number of added
    // values is 1 higher than the actual number of added values in the histogram's buckets. At high percentiles, it ends up
    // searching through all the buckets for the missing value, and stops at the end, resulting in the "infinity" value of Int.MaxValue.
    // The percentile calculation function keeps track of the last bucket with a non-zero count, and use that bucket instead of the
    // last one when such a situation occurs. This also ends up slightly changing how percentiles are calculated, but this change is
    // only apparent when relatively few values have been added to the histogram. This does not resolve the fact that the count is
    // still one off from the actual total count, but generally that will have little to no effect, especially when hundreds to
    // thousands of values are added per second.
    def p(num: Int, index: Int, build: Seq[Int], remain: Seq[Double], lastNonZeroIndex: Int): Seq[Int] =
      remain.headOption match {
        case None => build
        case Some(perc) => {
          if (perc <= 0.0 || count == 0 || ranges.isEmpty) {
            p(num, index, build :+ 0, remain.tail, lastNonZeroIndex)
          } else if (perc >= 1.0) {
            p(num, index, build :+ max.toInt, remain.tail, lastNonZeroIndex)
          } else {
            val percentileLimit: Int = ((count * perc) + 0.5).toInt //how many counts we need for this percentile
            val bucketCount          = mBuckets(index).get.toInt
            val newNum               = num + bucketCount
            //this is only needed because of a race condition where a value is
            //added while a percentile is calculated, this prevents accidentally
            //setting a percentile to Int.MaxValue when `count` > actual number of
            //values added
            val newLastNonZeroIndex = if (bucketCount > 0) index else lastNonZeroIndex
            if (newNum >= percentileLimit || index == ranges.size - 1) {
              val weightedValue = if (newLastNonZeroIndex < ranges.size - 1) {
                Math.min((ranges(newLastNonZeroIndex) + ranges(newLastNonZeroIndex + 1)) / 2, max.toInt)
              } else {
                infinity
              }
              p(num, index, build :+ weightedValue, remain.tail, newLastNonZeroIndex)
            } else {
              p(num + bucketCount, index + 1, build, remain, newLastNonZeroIndex)
            }
          }
        }
      }
    val sorted = percs.sortWith { _ < _ }
    sorted.zip(p(0, 0, Seq(), sorted, 0)).toMap
  }

  def percentile(perc: Double): Int = percentiles(Seq(perc))(perc)

  def metrics(address: MetricAddress, tags: TagMap, percs: Seq[Double]): MetricMap = {
    val others = Map("min" -> min, "max" -> max, "mean" -> mean).map {
      case (label, value) =>
        tags + ("label" -> label) -> value.toLong
    }

    Map(
      (address / "count") -> Map(tags -> count),
      address             -> (percentiles(percs).map { case (p, v) => tags + ("label" -> p.toString) -> v.toLong } ++ others)
    )
  }

}

//Working implementation of a Histogram
class DefaultHistogram private[metrics] (
    val address: MetricAddress,
    val percentiles: Seq[Double] = Histogram.defaultPercentiles,
    val sampleRate: Double = 1.0,
    val pruneEmpty: Boolean = false,
    val buckets: BucketList = Histogram.defaultBucketRanges,
    intervals: Seq[FiniteDuration]
) extends Histogram {

  val tagHists: Map[FiniteDuration, ConcurrentHashMap[TagMap, BaseHistogram]] = intervals.map { i =>
    val m = new ConcurrentHashMap[TagMap, BaseHistogram]
    i -> m
  }.toMap

  def add(value: Int, tags: TagMap = TagMap.Empty) {
    if (sampleRate >= 1.0 || ThreadLocalRandom.current.nextDouble(1.0) < sampleRate) {
      tagHists.foreach {
        case (_, taghists) =>
          Option(taghists.get(tags)) match {
            case Some(got) => got.add(value)
            case None =>
              taghists.putIfAbsent(tags, new BaseHistogram(buckets))
              //TODO: possible race condition if removed between these lines
              taghists.get(tags).add(value)
          }
      }
    }
  }

  private def withHist[T](collectionInterval: FiniteDuration, tags: TagMap)(op: BaseHistogram => T): Option[T] = {
    Option(tagHists(collectionInterval).get(tags)).map(op)
  }

  def percentile(collectionInterval: FiniteDuration, percent: Double, tags: TagMap = TagMap.Empty): Int = {
    withHist(collectionInterval, tags)(_.percentile(percent)).getOrElse(0)
  }

  def count(collectionInterval: FiniteDuration, tags: TagMap = TagMap.Empty): Int = {
    withHist(collectionInterval, tags)(_.count.toInt).getOrElse(0)
  }

  def tick(interval: FiniteDuration): MetricMap = {
    val taghist = tagHists(interval)
    val keys    = taghist.keys
    val build   = scala.collection.mutable.Map[MetricAddress, ValueMap]()
    while (keys.hasMoreElements) {
      val key  = keys.nextElement
      val hist = taghist.get(key)
      if (hist.count == 0 && pruneEmpty) {
        //there is obviously a race condition here where another thead could be
        //simultaneously hitting this histogram, but basically all that happens
        //is one value is lost
        taghist.remove(key)
      } else {
        val keymap = hist.metrics(address, key, percentiles)
        keymap.foreach {
          case (addr, values) =>
            if (build contains addr) {
              build(addr) = build(addr) ++ values
            } else {
              build(addr) = values
            }
        }
      }
      hist.tick()
    }
    build.toMap
  }
}

//Dummy implementation of a Histogram, used when "enabled=false" is specified at creation
class NopHistogram private[metrics] (val address: MetricAddress,
                                     val percentiles: Seq[Double] = Histogram.defaultPercentiles,
                                     val sampleRate: Double = 1.0,
                                     val pruneEmpty: Boolean = false,
                                     val buckets: BucketList = BucketList(Vector()))
    extends Histogram {
  val empty: MetricMap                                   = Map()
  override def tick(interval: FiniteDuration): MetricMap = empty

  override def add(value: Int, tags: TagMap): Unit = {}

  def percentile(collectionInterval: FiniteDuration, percent: Double, tags: TagMap = TagMap.Empty): Int = {
    0
  }

  def count(collectionInterval: FiniteDuration, tags: TagMap = TagMap.Empty): Int = {
    0
  }
}
