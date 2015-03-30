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
 * @param metricAddress The MetricAddress of the MetricSystem that this reporter is a member
 * @param metricSenders A list of [[MetricSender]] instances that the reporter will use to send metrics
 * @param globalTags
 * @param filters
 * @param includeHostInGlobalTags
 */
case class MetricReporterConfig(
  metricAddress: MetricAddress,
  metricSenders: Seq[MetricSender],
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

  private val strippedAddress = metricAddress.toString.replace("/", "")

  private def createSender(sender : MetricSender) = context.actorOf(sender.props, name = s"$strippedAddress-${sender.name}-sender")

  private var reporters = Seq[ActorRef]()

  private val compiledGlobalTags = {
    val userTags = globalTags.map{_.tags}.getOrElse(Map())
    val added = if (includeHostInGlobalTags) Map("host" -> localHostname) else Map()
    userTags ++ added
  }

  def receive = {

    case ReportMetrics(m) => {

      val filtered: MetricMap = filters.fold(m)(m.filter(_))
      val s = MetricSender.Send(filtered, compiledGlobalTags, System.currentTimeMillis())
      sendToReporters(s)
    }
    case ResetSender => {
      log.info("resetting stats senders")
      sendToReporters(PoisonPill)
      reporters = metricSenders.map(createSender)
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
