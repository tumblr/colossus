package colossus.metrics

import java.util.concurrent.atomic.AtomicReference

import akka.actor._
import colossus.metrics.IntervalAggregator._
import colossus.metrics.logging.ColossusLogging

import scala.concurrent.duration._
import scala.collection.JavaConverters._

class IntervalAggregator(interval: FiniteDuration,
                         snapshot: AtomicReference[MetricMap],
                         sysMetricsNamespace: Option[MetricNamespace])
    extends Actor
    with ColossusLogging {

  import context.dispatcher

  private val systemMetrics = sysMetricsNamespace.map(new SystemMetricsCollector(_))

  private var collections = Set[Collection]()
  private var reporters   = Set[ActorRef]()

  def blankMap(): MetricMap = systemMetrics.map { _.metrics }.getOrElse(Map())

  def receive: Receive = {

    case Tick =>
      context.system.scheduler.scheduleOnce(interval, self, Tick)
      var build = blankMap()
      collections.foreach { collection =>
        build = build ++ collection.tick(interval)
      }

      snapshot.set(build)
      reporters.foreach { reporter =>
        reporter ! ReportMetrics(build)
      }

    case RegisterCollection(collection) =>
      collections = collections + collection

    case RegisterReporter(ref) =>
      reporters = reporters + ref
      context.watch(ref)

    case Terminated(child) =>
      if (reporters.contains(child)) {
        debug(s"oh no!  We lost a MetricReporter $child. Removing from registered reporters.")
        reporters = reporters - child
      } else {
        warn(s"someone: $child died..for which there is no reporter registered")
      }

    case ListReporters =>
      sender ! reporters
  }

  override def preStart() {
    context.system.scheduler.scheduleOnce(interval, self, Tick)
  }
}

object IntervalAggregator {
  case class RegisterReporter(ref: ActorRef)
  case class RegisterCollection(collection: Collection)
  case class ReportMetrics(m: MetricMap)
  private[metrics] case object ListReporters
  private[metrics] case object Tick

}

class SystemMetricsCollector(namespace: MetricNamespace) {

  import management._

  def metrics: MetricMap = {
    val runtime         = Runtime.getRuntime
    val maxMemory       = runtime.maxMemory
    val allocatedMemory = runtime.totalMemory
    val freeMemory      = runtime.freeMemory
    val memoryInfo: MetricMap = Map(
      (namespace.namespace / "system" / "memory") -> Map(
        Map("type" -> "max")       -> maxMemory,
        Map("type" -> "allocated") -> allocatedMemory,
        Map("type" -> "free")      -> freeMemory
      )
    )
    val gcInfo: MetricMap = {
      val beans = ManagementFactory.getGarbageCollectorMXBeans.asScala
      val (cyclesMetric, msecMetric) = beans.foldLeft((ValueMap.Empty, ValueMap.Empty)) {
        case ((cycles, msec), tastyBean: management.GarbageCollectorMXBean) =>
          val tags = Map("type" -> tastyBean.getName.replace(' ', '_'))
          (cycles + (tags -> tastyBean.getCollectionCount), msec + (tags -> tastyBean.getCollectionTime))
      }
      Map(
        (namespace.namespace / "system" / "gc" / "cycles") -> cyclesMetric,
        (namespace.namespace / "system" / "gc" / "msec")   -> msecMetric
      )
    }

    val fdInfo: MetricMap = ManagementFactory.getOperatingSystemMXBean match {
      case u: com.sun.management.UnixOperatingSystemMXBean =>
        Map(
          (namespace.namespace / "system" / "fd_count") -> Map(
            Map.empty[String, String] -> u.getOpenFileDescriptorCount)
        )
      case _ => MetricMap.Empty //for those poor souls using non-*nix
    }

    (memoryInfo ++ gcInfo ++ fdInfo).withTags(namespace.tags)
  }
}


