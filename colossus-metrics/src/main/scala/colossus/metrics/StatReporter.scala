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
 * @param metricSenders A list of [[MetricSender]] instances that the reporter will use to send metrics
 * @param globalTags
 * @param filters
 * @param includeHostInGlobalTags
 */
case class MetricReporterConfig(
  metricSenders: Seq[MetricSender],
  globalTags: Option[TagGenerator] = None,
  filters: MetricReporterFilter = MetricReporterFilter.All,
  includeHostInGlobalTags: Boolean = true
)

class MetricReporter(intervalAggregator : ActorRef, config: MetricReporterConfig, metricSystemName: String) extends Actor with ActorLogging{
  import MetricReporter._
  import config._

  val localHostname = java.net.InetAddress.getLocalHost.getHostName

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 1, withinTimeRange = 3 seconds) {
    case _: NullPointerException     => Escalate
    case _: Exception                => Restart
  }

  private def createSender(sender : MetricSender) = context.actorOf(sender.props, name = s"$metricSystemName-${sender.name}-sender")

  private var reporters = Seq[ActorRef]()

  private def compiledGlobalTags() = {
    val userTags = globalTags.map{_.tags}.getOrElse(Map())
    val added = if (includeHostInGlobalTags) Map("host" -> localHostname) else Map()
    userTags ++ added
  }

  def receive = {

    case ReportMetrics(m) => {
      val s = MetricSender.Send(filterMetrics(m), compiledGlobalTags(), System.currentTimeMillis())
      sendToReporters(s)
    }
    case ResetSender => {
      log.info("resetting stats senders")
      sendToReporters(PoisonPill)
      reporters = metricSenders.map(createSender)
    }
  }

  private def filterMetrics(m : MetricMap) : MetricMap = {
    filters match {
      case MetricReporterFilter.All => m
      case MetricReporterFilter.WhiteList(x) => m.filterKeys(k => x.exists(_.matches(k)))
      case MetricReporterFilter.BlackList(x) => m.filterKeys(k => !x.exists(_.matches(k)))
    }
  }

  private def sendToReporters(a : Any){
    reporters.foreach(_ ! a)
  }

  override def preStart() {
    reporters = metricSenders.map(createSender)
    intervalAggregator ! RegisterReporter(self)
  }
}

object MetricReporter {
  case object ResetSender

  def apply(config: MetricReporterConfig, intervalAggregator : ActorRef, name: String)(implicit fact: ActorRefFactory): ActorRef = {
    fact.actorOf(Props(classOf[MetricReporter], intervalAggregator, config, name))
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

/**
 * Tells a MetricReporter how to filter its Metrics before handing off to a Sender.
 */
sealed trait MetricReporterFilter

object MetricReporterFilter {

  /**
   * Do no filtering, pass all metrics through
   */
  case object All extends MetricReporterFilter

  /**
   * Only allow metrics for the specified MetricAddresses
   * @param addresses
   */
  case class WhiteList(addresses : Seq[MetricAddress]) extends MetricReporterFilter

  /**
   * Allow all other metrics except for the ones in the specified MetricAddresses
   * @param addresses
   */
  case class BlackList(addresses : Seq[MetricAddress]) extends MetricReporterFilter
}
