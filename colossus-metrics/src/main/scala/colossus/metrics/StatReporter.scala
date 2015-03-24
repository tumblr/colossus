package colossus.metrics

import akka.actor.SupervisorStrategy._
import akka.actor.{OneForOneStrategy, _}
import colossus.metrics.IntervalAggregator.{RegisterReporter, ReportMetrics}

import scala.concurrent.duration._

trait TagGenerator {
  def tags: TagMap
}

/**
 * Configuration class for the metric reporter
 * @param metricSender A [[MetricSender]] instance that the reporter will use to send metrics
 * @param globalTags
 * @param filters
 * @param includeHostInGlobalTags
 */
case class MetricReporterConfig(
  metricSender: MetricSender,
  globalTags: Option[TagGenerator] = None,
  filters: Option[Seq[MetricFilter]] = None,
  includeHostInGlobalTags: Boolean = true
)

class MetricReporter(intervalAggregator : ActorRef, config: MetricReporterConfig) extends Actor with ActorLogging{
  import MetricReporter._
  import config._

  val localHostname = java.net.InetAddress.getLocalHost.getHostName

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 1, withinTimeRange = 3 seconds) {
    case _: NullPointerException     => Escalate
    case _: Exception                => Restart
  }

  def createSender() = context.actorOf(metricSender.props, name = s"${metricSender.name}-sender")
  var statSender = createSender()

  def getGlobalTags = {
    val userTags = globalTags.map{_.tags}.getOrElse(Map())
    val added = if (includeHostInGlobalTags) Map("host" -> localHostname) else Map()
    userTags ++ added
  }

  def receive = {

    case ReportMetrics(m) => {
      val filtered: MetricMap = filters.fold(m)(m.filter(_))
      statSender ! MetricSender.Send(filtered, getGlobalTags, System.currentTimeMillis)
    }
    case ResetSender => {
      log.info("resetting stats sender")
      statSender ! PoisonPill
      statSender = createSender()
    }
  }

  override def preStart() {
    intervalAggregator ! RegisterReporter(self)
  }
}

object MetricReporter {
  case object ResetSender

  def apply(config: MetricReporterConfig, intervalAggregator : ActorRef)(implicit fact: ActorRefFactory): ActorRef = {
    fact.actorOf(Props(classOf[MetricReporter], intervalAggregator, config))
  }

}

trait MetricSender {
  def name: String
  def props: Props
}

object MetricSender {
  case class Send(metrics: MetricMap, globalTags: TagMap, timestamp: Long) {
    def fragments = metrics.fragments(globalTags)
  }
}
