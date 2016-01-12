package colossus.metrics

import akka.actor._

import scala.concurrent.duration._

class Rate(val address: MetricAddress)(implicit collection: Collection) extends Collector {

  val maps = collection.config.intervals.map{i => (i, new CollectionMap[TagMap])}.toMap

  def hit(tags: TagMap = TagMap.Empty, num: Long = 1) {
    maps.foreach{ case (_, map) => map.increment(tags) }
  }

  def tick(interval: FiniteDuration): MetricMap  = {
    Map(address -> maps(interval).snapshot(false, true))
  }
    

}


