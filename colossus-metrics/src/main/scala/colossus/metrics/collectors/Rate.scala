package colossus.metrics

import scala.concurrent.duration._

class Rate(val address: MetricAddress)(implicit collection: Collection) extends Collector {

  private val maps = collection.config.intervals.map{i => (i, new CollectionMap[TagMap])}.toMap

  //note - the totals are shared amongst all intervals, and we use the smallest
  //interval to update them
  private val totals = new CollectionMap[TagMap]
  private val minInterval = collection.config.intervals.min

  def hit(tags: TagMap = TagMap.Empty, num: Long = 1) {
    maps.foreach{ case (_, map) => map.increment(tags) }
  }

  def tick(interval: FiniteDuration): MetricMap  = {
    val snap = maps(interval).snapshot(false, true)
    if (interval == minInterval) {
      snap.foreach{ case (tags, value) => totals.increment(tags, value) }
    }
    Map(address -> snap, address / "count" -> totals.snapshot(false, false))
  }
    

}


