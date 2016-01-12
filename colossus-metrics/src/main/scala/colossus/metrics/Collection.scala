package colossus.metrics

import akka.actor._

import scala.concurrent.duration._
import scala.reflect.ClassTag

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong


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
 */
case class CollectorConfig(intervals: Seq[FiniteDuration])

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
        map.putIfAbsent(tags, new AtomicLong(num))
      }
    }
  }

  def set(tags: T, num: Long) {
    Option(map.get(tags)) match {
      case Some(got) => got.set(num)
      case None => {
        map.putIfAbsent(tags, new AtomicLong(num))
      }
    }
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
      build = build + (key -> value)
    }

    build
  }

}

class DuplicateMetricException(message: String) extends Exception(message)

class Collection(val config: CollectorConfig) {

  val collectors = new ConcurrentHashMap[MetricAddress, Collector]

  def add(collector: Collector) {
    collectors.put(collector.address, collector)
  }

  def getOrAdd[T <: Collector : ClassTag](created: T): T = {
    collectors.putIfAbsent(created.address, created) match {
      case null => created
      case exists: T => exists
      case other => throw new DuplicateMetricException(s"An event collector of type ${other.getClass.getSimpleName} already exists")
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
