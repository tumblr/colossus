package colossus.metrics

import scala.concurrent.duration._

class Counter private[metrics](val address: MetricAddress)(implicit collection: Collection) extends Collector {

  private val counters = new CollectionMap[TagMap]

  def increment(tags: TagMap = TagMap.Empty, num: Long = 1) {
    counters.increment(tags, num)
  }

  def decrement(tags: TagMap = TagMap.Empty, num: Long = 1) = increment(tags, 0 - num)

  def set(num: Long, tags: TagMap = TagMap.Empty) {
    counters.set(tags, num)
  }

  def get(tags: TagMap = TagMap.Empty): Long = counters.get(tags).getOrElse(0)

  def tick(interval: FiniteDuration): MetricMap  = {
    val values = counters.snapshot(false, false)
    if (values.isEmpty) Map() else Map(address -> values)
  }
    

}

object Counter {

  def apply(address: MetricAddress)(implicit collection: Collection): Counter = {
    collection.getOrAdd(new Counter(address))
  }
}
