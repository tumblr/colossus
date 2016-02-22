package colossus.metrics

import scala.concurrent.duration._

class Rate private[colossus](val address: MetricAddress)(implicit collection: Collection) extends Collector {

  private val maps = collection.config.intervals.map{i => (i, new CollectionMap[TagMap])}.toMap

  //note - the totals are shared amongst all intervals, and we use the smallest
  //interval to update them
  private val totals = new CollectionMap[TagMap]
  private val minInterval = if (collection.config.intervals.size > 0) collection.config.intervals.min else Duration.Inf

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

object Rate {

  def apply(address: MetricAddress)(implicit collection: Collection): Rate = {
    collection.getOrAdd(new Rate(address))
  }
}


