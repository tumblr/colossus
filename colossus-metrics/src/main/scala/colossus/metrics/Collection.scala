package colossus.metrics

import com.typesafe.config.{ConfigFactory, Config}

import scala.concurrent.duration._

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.reflect.ClassTag

/**
  * A config object passed to new event collectors in addition to their own config.
 *
 * @param intervals The aggregation intervals configured for the MetricSystem this collection belongs to
 * @param config A typesafe config object that contains all the config for collectors configured using typesafe config
 * @param collectorDefaults a typesafe config object for collector defaults
 */
case class CollectorConfig(intervals: Seq[FiniteDuration], baseConfig : Config, collectorDefaults: Config) {

  /**
    * Build a Config for a collector by stacking config objects specified by the paths elements
    *
    * The order of resolution is: metric-address-based path, alternate path, collector defaults
    *
    * @param address Address of the Metric, turned into a path and used as a config source.
    * @param defaultsPath the path to the defaults in the collectorDefaults config
    * @param alternatePaths Ordered list of config paths, ordered by highest precedence.
    * @return
    */
  def resolveConfig(address : MetricAddress, defaultsPath: String, alternatePaths: String*) : Config = {
    import ConfigHelpers._
    baseConfig
      .withFallbacks((address.configString +: alternatePaths):_*)
      .withFallback(collectorDefaults.getConfig(defaultsPath))
  }

}

/**
  * Base trait required by all metric types.
  */
trait Collector {

  /**
    * The MetricAddress of this Collector.  Note, this will be relative to the containing MetricSystem's metricAddress.
    *
    * @return
    */
  def address: MetricAddress

  /**
    * TODO
    *
    * @param interval
    * @return
    */
  def tick(interval: FiniteDuration): MetricMap
}

private[metrics] class CollectionMap[T] {

  private val map = new ConcurrentHashMap[T, AtomicLong]

  def update(tags: T, num: Long, op: AtomicLong => Long => Unit) {
    Option(map.get(tags)) match {
      case Some(got) => op(got)(num)
      case None => {
        Option(map.putIfAbsent(tags, new AtomicLong(num))).foreach{got => op(got)(num)}          
      }
    }

  }

  def increment(tags: T, num: Long = 1) {
    update(tags, num, _.addAndGet _)
  }

  def set(tags: T, num: Long) {
    update(tags, num, _.set _)
  }

  def get(tags: T): Option[Long] = Option(map.get(tags)).map{_.get}

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
      if (!(pruneEmpty &&  value == 0)) {
        build = build + (key -> value)
      }
    }

    build
  }

}

class DuplicateMetricException(message: String) extends Exception(message)

class Collection(val config: CollectorConfig) {

  import Collection.TaggedCollector
  val collectors : ConcurrentHashMap[MetricAddress, TaggedCollector] = new ConcurrentHashMap[MetricAddress, TaggedCollector]()

  /**
   * Retrieve a collector of a specific type by address, creating a new one if
   * it does not exist.  If an existing collector of a different type already
   * exists, a `DuplicateMetricException` will be thrown.
   * @param address Address meant to be relative to this MetricNamespace's namespace
   * @param f Function which takes in an absolutely pathed MetricAddress, and a [[CollectorConfig]] and returns an instance of a [[Collector]]
    */
  def getOrAdd[T <: Collector : ClassTag](address : MetricAddress, tags: TagMap)(f : (MetricAddress, CollectorConfig) => T): T = {
    def cast(retrieved: Collector): T = retrieved match {
      case t : T => t
      case other => {
        throw new DuplicateMetricException(
          s"An event collector with address $address of type ${other.getClass.getSimpleName} already exists"
        )
      }
    }
    if (collectors.containsKey(address)) {
      cast(collectors.get(address).collector)
    } else {
      val c = f(address, config)
      collectors.putIfAbsent(address, TaggedCollector(c, tags)) match {
        case null => c
        case other => cast(other.collector)
      }
    }
  }

  def tick(interval: FiniteDuration): MetricMap = {
    val keys = collectors.keys
    var build: MetricMap = Map()
    while (keys.hasMoreElements) {
      val key = keys.nextElement
      Option(collectors.get(key)) foreach { c =>
        build = build ++ c.collector.tick(interval).mapValues { valueMap =>
          valueMap.map { case (t, tValue) => (t ++ c.tagMap, tValue) }
        }
      }
    }
    build
  }


}

//Used as a convenience function in tests.  Used in both both colossus-tests and in colossus-metrics tests, which means
//this project is the lowest common denominator for both.
object Collection{
  def withReferenceConf(intervals : Seq[FiniteDuration]) : Collection = {
    val config = ConfigFactory.defaultReference().getConfig(MetricSystemConfig.ConfigRoot)
    new Collection(CollectorConfig(intervals,config, config.getConfig("system.collector-defaults") ))
  }

  case class TaggedCollector(collector: Collector, tagMap: TagMap)
}
