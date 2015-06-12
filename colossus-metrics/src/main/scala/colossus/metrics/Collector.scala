package colossus.metrics

import akka.actor._

class Collector(val metricSystem: MetricSystem, val collection: LocalCollection) extends Actor with ActorMetrics{

  override def globalTags = collection.globalTags

  override lazy val metrics = collection

  def receive = handleMetrics

  
}


