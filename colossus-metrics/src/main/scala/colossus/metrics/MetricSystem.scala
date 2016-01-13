package colossus.metrics

import akka.actor._
import akka.agent.Agent
import colossus.metrics.MetricAddress.Root

import scala.concurrent.duration._

class MetricInterval private[metrics](val namespace : MetricAddress, val interval : FiniteDuration, val intervalAggregator : ActorRef, snapshot : Agent[MetricMap]) {

    /**
   * The latest metrics snapshot
   * @return
   */
  def last : MetricMap = snapshot.get()

  /**
   * Attach a reporter to this MetricPeriod.
   * @param config  The [[MetricReporterConfig]] used to configure the [[MetricReporter]]
   * @return
   */
  def report(config : MetricReporterConfig)(implicit fact: ActorRefFactory) : ActorRef = MetricReporter(config, intervalAggregator)
}

/**
 * The MetricSystem is a set of actors which handle the background operations of dealing with metrics. In most cases,
 * you only want to have one MetricSystem per application.
 *
 * Metrics are generated periodically by a Tick message published on the global event bus. By default this happens once
 * per second, but it can be configured to any time interval. So while events are being collected as they occur,
 * compiled metrics (such as rates and histogram percentiles) are generated once per tick.
 *
 * @param namespace the base of the url describing the location of metrics within the system
 * @param metricIntervals Map of all MetricIntervals for which this system collects Metrics.
 */
case class MetricSystem private [metrics](namespace: MetricAddress, metricIntervals : Map[FiniteDuration, MetricInterval]) {

  implicit val base = new Collection(CollectorConfig(metricIntervals.keys.toSeq))
  registerCollection(base)

  def registerCollection(collection: Collection): Unit = {
    metricIntervals.values.foreach(_.intervalAggregator ! IntervalAggregator.RegisterCollection(collection))
  }

}

object MetricSystem {
  /**
   * Constructs a metric system
   * @param namespace the base of the url describing the location of metrics within the system
   * @param metricIntervals How often to report metrics
   * @param collectSystemMetrics whether to collect metrics from the system as well
   * @param system the actor system the metric system should use
   * @return
   */
  def apply(namespace: MetricAddress, metricIntervals: Seq[FiniteDuration] = Seq(1.second, 1.minute), collectSystemMetrics: Boolean = true)
  (implicit system: ActorSystem): MetricSystem = {
    import system.dispatcher


    val m : Map[FiniteDuration, MetricInterval] = metricIntervals.map{ interval =>
      val snap = Agent[MetricMap](Map())
      val aggregator : ActorRef = createIntervalAggregator(system, namespace, interval, snap, collectSystemMetrics)
      val i = new MetricInterval(namespace, interval, aggregator, snap)
      interval -> i
    }.toMap

    MetricSystem(namespace, m)
  }

  private def createIntervalAggregator(system : ActorSystem, namespace : MetricAddress, interval : FiniteDuration,
                                       snap : Agent[MetricMap], collectSystemMetrics : Boolean) = {
    system.actorOf(Props(classOf[IntervalAggregator], namespace, interval, snap, collectSystemMetrics))
  }

  def deadSystem(implicit system: ActorSystem) = {
    MetricSystem(Root / "DEAD", Map[FiniteDuration, MetricInterval]())
  }

}



