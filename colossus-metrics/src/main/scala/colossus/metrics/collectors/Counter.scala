package colossus.metrics

import scala.concurrent.duration._

class Counter(val address: MetricAddress)(implicit collection: Collection) extends Collector(collection) {

  private val counters = new CollectionMap[TagMap]

  def increment(tags: TagMap = TagMap.Empty, num: Long = 1) {
    counters.increment(tags, num)
  }

  def decrement(tags: TagMap = TagMap.Empty, num: Long = 1) = increment(tags, 0 - num)

  def set(tags: TagMap = TagMap.Empty, num: Long) {
    counters.set(tags, num)
  }

  def get(tags: TagMap = TagMap.Empty): Long = counters.get(tags).getOrElse(0)

  def tick(interval: FiniteDuration): MetricMap  = {
    Map(address -> counters.snapshot(false, false))
  }
    

}
