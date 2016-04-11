package colossus.metrics

import com.typesafe.config.{ConfigFactory, Config}

import scala.concurrent.duration._

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.reflect.ClassTag

/**
  * This is passed to new event collectors in addition to their own config.
 *
 * TODO: we might want to include global tags in here as well, and remove them
 * from CollectionContext.  This would mean event collectors would be
 * constructed with global tagsinstead of them being passed in during
 * collection, but right now that basically already happens since the tags are
 * passed in during the collection's construction and then it passes it to each
 * collector
 *
 * @param intervals The aggregation intervals configured for the MetricSystem this collection belongs to
 * @param config The Configuration of the underlying [[MetricSystem]]
 */
case class CollectorConfig(intervals: Seq[FiniteDuration], config : Config = ConfigFactory.defaultReference())

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
        map.putIfAbsent(tags, new AtomicLong(num))
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

  val collectors = new ConcurrentHashMap[MetricAddress, Collector]

  //not used
  def add(collector: Collector) {
    collectors.put(collector.address, collector)
  }

  /**
   * Retrieve a collector of a specific type by address, creating a new one if
   * it does not exist.  If an existing collector of a different type already
   * exists, a `DuplicateMetricException` will be thrown.
   * @param address Address meant to be relative to this MetricNamespace's namespace
   * @param f Function which takes in an absolutely pathed MetricAddress, and a [[CollectorConfig]] and returns an instance of a [[Collector]]
    */
  def getOrAdd[T <: Collector : ClassTag](address : MetricAddress)(f : (MetricAddress, CollectorConfig) => T): T = {
    def cast(retrieved: Collector): T = retrieved match {
      case t : T => t
      case other => {
        throw new DuplicateMetricException(
          s"An event collector with address $address of type ${other.getClass.getSimpleName} already exists"
        )
      }
    }
    if (collectors.containsKey(address)) {
      cast(collectors.get(address))
    } else {
      val c = f(address, config)
      collectors.putIfAbsent(address, c) match {
        case null => c
        case other => cast(other)
      }
    }
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
