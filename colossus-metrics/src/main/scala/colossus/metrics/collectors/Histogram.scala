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
  sampleRate: Double = 1.0
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
    sampleRate: Double = 1.0
  ) : HistogramParams = HistogramParams(address, bucketRanges, percentiles, sampleRate)

  implicit object HistogramGenerator extends Generator[Histogram, HistogramParams] {
    def local(params: HistogramParams, config: CollectorConfig) = new PeriodicHistogram(params, config)
    def shared(params: HistogramParams, config: CollectorConfig)(implicit actor: ActorRef) = new SharedHistogram(params, actor)
  }

}

case class Snapshot(min: Int, max: Int, count: Int, percentiles: Map[Double, Int]) {
  def infoString = s"Count: $count\nMin: $min\nMax: $max\n" + percentiles.map{case (perc, value) => s"${perc * 100}%: $value"}.mkString("\n")

  import MetricValues._

  def metrics(address: MetricAddress, tags: TagMap): MetricMap = Map (
    (address / "min") -> Map(tags -> MinValue(min)),
    (address / "max") -> Map(tags -> MaxValue(max)),
    (address / "count") -> Map(tags -> SumValue(count)),
    address -> percentiles.map{case (p, v) => tags + ("percentile" -> p.toString) -> WeightedAverageValue(weight = count, value = v.toLong)}
  )

}
object Snapshot {

  def zero(percs: Seq[Double]) = Snapshot(0,0,0, percs.map{_ -> 0}.toMap)

}

/**
 * This is the actual histogram data structure.  It knows nothing of tags or metrics
 */
class BaseHistogram(val bucketList: BucketList = Histogram.defaultBucketRanges) {
  require (ranges.size > 1, "histogram must have at least 2 buckets")

  private lazy val ranges = bucketList.buckets

  private val infinity   = ranges.last
  private val mBuckets = new Array[Int](ranges.size)

  private var mMax = 0
  private var mMin = infinity
  private var mCount = 0

  def min = if (count > 0) mMin else 0
  def max = mMax
  def count = mCount
  def buckets: Seq[Int] = mBuckets

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

  def reset() {
    mMin = infinity
    mMax = 0
    mCount = 0
    (0 until mBuckets.size).foreach{i =>
      mBuckets(i) = 0
    }
  }

  def add(value: Int) {
    require(value >= 0, "value cannot be negative")
    mCount += 1
    mMax = math.max(mMax, value)
    mMin = math.min(mMin, value)
    mBuckets(bucketFor(value)) += 1
  }

  def percentiles(percs: Seq[Double]): Map[Double, Int] =  {
    def p(num: Int, index: Int, build: Seq[Int], remain: Seq[Double]): Seq[Int] = remain.headOption match {
      case None => build
      case Some(perc) => {
        if (perc <= 0.0 || mCount == 0) {
          p(num, index, build :+ 0, remain.tail)
        } else if (perc >= 1.0) {
          p(num, index, build :+ mMax, remain.tail)          
        } else {
          if (index < mBuckets.size - 1 && num < mCount * perc) {
            p(num + mBuckets(index), index + 1, build, remain)
          } else {
            val res = ((ranges(index - 1) + ranges(index)) / 2).toInt
            p(num, index, build :+ res, remain.tail)
          }
        }
      }
    }
    val sorted = percs.sortWith{_ < _}
    sorted.zip(p(0, 0, Seq(), sorted)).toMap
  }

  def percentile(perc: Double): Int = percentiles(Seq(perc))(perc)

  def snapshot(percs: List[Double]) = Snapshot(min, max, count, percentiles(percs))

}

class TaggedHistogram(val ranges: BucketList) {

  private val hists = collection.mutable.Map[TagMap, BaseHistogram]()

  def add(value: Int, tags: TagMap = TagMap.Empty) {
    if (!hists.contains(tags)) {
      hists(tags) = new BaseHistogram(ranges)
    }
    hists(tags).add(value)
  }

  def tick() {
    reset()
  }

  def reset() {
    hists.foreach{case (tags, h) => h.reset()}
  }

  def apply(tags: TagMap): BaseHistogram = hists(tags)


  def snapshots(percs: List[Double]): Map[TagMap, Snapshot] = hists.map{case (tags, hist) => (tags, hist.snapshot(percs))}.toMap


}

/**
 * A periodic histogram multiplexes a histogram into several with different periods of resetting
 *
 * Ticks are controlled externally so we can ensure that we get a complete set
 * of data before resetting the hists
 */
class PeriodicHistogram(params: HistogramParams, config: CollectorConfig) extends Histogram with TickedCollector with LocalLocality {
  def address = params.address

  import PeriodicHistogram._

  val hists: Map[FiniteDuration, TaggedHistogram] = config.intervals.map{interval => 
    interval -> new TaggedHistogram(params.bucketRanges)
  }.toMap
  var lastSnapshot: MetricMap = MetricMap.Empty

  private val sampleMod = (100 / (params.sampleRate * 100)).toInt
  private var sampleTicker = 0


  private def sampleTick(): Boolean = {
    sampleTicker += 1
    if (sampleTicker == sampleMod) {
      sampleTicker = 0
      true
    } else {
      false
    }
  }

  def add(value: Int, tags: TagMap) = {
    if (sampleTick()) hists.foreach{case (period, hist) => hist.add(value, tags)}
  }

  def tick(period: FiniteDuration) {
    hists(period).tick()
  }

  def metrics(context: CollectionContext): MetricMap = {    
    hists(context.interval).snapshots(params.percentiles).foldLeft[MetricMap](Map()){ case (build, (tags, snapshot)) =>
      build <+> snapshot.metrics(params.address, tags ++ context.globalTags)
    }
  }

  def reset() {
    hists.foreach{case (_, hist) => hist.reset()}
  }

  def event = {
    case Add(_, t, v) => add(v, t)
  }

}

class SharedHistogram(params: HistogramParams, collector: ActorRef) extends Histogram with SharedLocality {
  def address = params.address
  def add(value: Int, tags: TagMap = TagMap.Empty) {
    collector ! PeriodicHistogram.Add(params.address, tags, value)
  }
}


object PeriodicHistogram {

  case class Add(address: MetricAddress, tags: TagMap, value: Int) extends MetricEvent

  val INF = Int.MaxValue

  
}
