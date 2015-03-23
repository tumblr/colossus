package colossus.metrics


import akka.actor._

import LocalCollection._

import IntervalAggregator._

trait ActorMetrics extends Actor with ActorLogging {
  val metricSystem: MetricSystem

  def globalTags: TagMap = TagMap.Empty

  val metrics = new LocalCollection(MetricAddress.Root, globalTags)

  def handleMetrics: Receive = {
    case m : MetricEvent => metrics.handleEvent(m) match {
      case Ok => {}
      case UnknownMetric => log.error(s"Event for unknown Metric: $m")
      case InvalidEvent => log.error(s"Invalid event $m")
    }
    case Tick(v) => {
      val agg = metrics.aggregate
      metrics.tick(metricSystem.tickPeriod)
      sender() ! Tock(agg, v)
    }
  }

  override def preStart() {
    metricSystem.intervalAggregator ! IntervalAggregator.RegisterCollector(self)
  }
}
