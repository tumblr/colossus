package colossus.metrics


import akka.actor._

import LocalCollection._

import MetricClock._

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
    case Tick(id, v) if (id == metricSystem.id) => {
      val agg = metrics.aggregate
      metrics.tick(metricSystem.tickPeriod)
      metricSystem.database ! Tock(agg, v)
    }
  }

  override def preStart() {
    context.system.eventStream.subscribe(self, classOf[Tick])
    metricSystem.database ! MetricDatabase.RegisterCollector(self)
  }
}
