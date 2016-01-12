package colossus.metrics

import akka.actor._

import scala.concurrent.duration._

import EventLocality._

class Rate(val address: MetricAddress, config: CollectorConfig) extends Collector {

  val maps = config.intervals.map{i => (i, new CollectionMap[TagMap])}.toMap

  def hit(tags: TagMap = TagMap.Empty, num: Long = 1) {
    maps.foreach{ case (_, map) => map.increment(tags) }
  }

  def tick(interval: FiniteDuration): MetricMap  = {
    Map(address -> maps(interval).snapshot(false, true))
  }
    

}


