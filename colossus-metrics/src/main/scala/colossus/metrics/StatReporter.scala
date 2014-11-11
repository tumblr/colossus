package colossus.metrics

import akka.actor._
import akka.pattern.ask
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy._
import akka.util.Timeout

import scala.concurrent.duration._
import scala.util.{Success, Failure}

import java.io._

trait TagGenerator {
  def tags: TagMap
}

case class MetricReporterConfig(
  metricSender: MetricSender,
  globalTags: Option[TagGenerator] = None,
  filters: Option[Seq[MetricFilter]] = None,
  frequency: FiniteDuration = 60.seconds,
  includeHostInGlobalTags: Boolean = true
)

class MetricReporter(metrics: MetricSystem, config: MetricReporterConfig) extends Actor with ActorLogging{
  import MetricReporter._
  import config._
  import context.dispatcher

  val localHostname = java.net.InetAddress.getLocalHost.getHostName

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 1, withinTimeRange = 3 seconds) {
    case _: NullPointerException     => Escalate
    case _: IOException              => Restart
    case _: Exception                => Restart
  }

  def createSender() = context.actorOf(metricSender.props, name = s"${metricSender.name}-sender")
  var statSender = createSender()

  implicit val timeout = Timeout(100.milliseconds)

  def getGlobalTags = {
    val usertags = globalTags.map{_.tags}.getOrElse(Map())
    val added = if (includeHostInGlobalTags) Map("host" -> localHostname) else Map()
    usertags ++ added
  }

  def receive = {
    case SendStats => (metrics.database ? MetricDatabase.GetDB).onComplete{
      case Success(MetricDatabase.DB(db)) => {
        val filtered: MetricMap = filters.map{f => db.filter(f)}.getOrElse(db)
        statSender ! MetricSender.Send(filtered, getGlobalTags, System.currentTimeMillis)
      }
      case Failure(whoops) => log.error(s"Metrics reporting failed: ${whoops.getMessage}")
      case _ => log.error("Bad response from db")
    }
    case ResetSender => {
      log.info("resetting stats sender")
      statSender ! PoisonPill
      statSender = createSender()
    }
      
  }

  override def preStart() {
    context.system.scheduler.schedule(frequency, frequency, self , SendStats)
  }

}

object MetricReporter {
  case object ResetSender
  case object SendStats

  def apply(config: MetricReporterConfig)(implicit metrics: MetricSystem, fact: ActorRefFactory): ActorRef = {
    fact.actorOf(Props(classOf[MetricReporter], metrics, config))
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
