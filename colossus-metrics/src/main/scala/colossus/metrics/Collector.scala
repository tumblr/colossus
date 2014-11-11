package colossus.metrics

import java.util.concurrent.ConcurrentHashMap

import akka.actor._

import EventLocality._

class Collector(val metricSystem: MetricSystem, val collectors: ConcurrentHashMap[MetricAddress, Local[EventCollector]], val gTags: TagMap) extends Actor with ActorMetrics{

  override def globalTags = gTags

  override val metrics = new LocalCollection(MetricAddress.Root, globalTags, collectors)(self)

  def receive = handleMetrics

  
}


