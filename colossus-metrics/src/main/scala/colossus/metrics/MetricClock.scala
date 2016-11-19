package colossus.metrics

import akka.actor._
import akka.agent.Agent

import scala.concurrent.duration._


class IntervalAggregator(interval: FiniteDuration, snapshot: Agent[MetricMap], sysMetricsNamespace: Option[MetricNamespace]) extends Actor with ActorLogging {

  import context.dispatcher
  import IntervalAggregator._
  import java.util.{HashSet=>JHashSet}
  import scala.collection.JavaConversions._

  val systemMetrics = sysMetricsNamespace.map{ns => new SystemMetricsCollector(ns)}

  def blankMap(): MetricMap = systemMetrics.map{_.metrics}.getOrElse( Map())

  var collections = Set[Collection]()
  var reporters = Set[ActorRef]()

  def receive = {

    case Tick => {
      context.system.scheduler.scheduleOnce(interval, self, Tick)
      var build = blankMap()
      collections.foreach{ collection =>
        build = build ++ collection.tick(interval)
      }

      snapshot.send(build)
      reporters.foreach{ reporter =>
        reporter ! ReportMetrics(build)
      }
    }

    case RegisterCollection(collection) => {
      collections = collections + collection
    }

    case RegisterReporter(ref) => {
      reporters = reporters + ref
      context.watch(ref)
    }

    case Terminated(child) => {
      if(reporters.contains(child)){
        log.debug(s"oh no!  We lost a MetricReporter $child. Removing from registered reporters.")
        reporters.remove(child)
      }else{
        log.warning(s"someone: $child died..for which there is no reporter registered")
      }
    }

    case ListReporters => {
      sender ! reporters.toSet //yea..that's right..immutable on the way out.
    }
  }

  override def preStart() {
    context.system.scheduler.scheduleOnce(interval, self, Tick)
  }
}

object IntervalAggregator {

  case class RegisterReporter(ref : ActorRef)
  case class RegisterCollection(collection: Collection)
  case class ReportMetrics(m : MetricMap)
  private[metrics] case object ListReporters
  private[metrics] case object Tick

}


class SystemMetricsCollector(namespace: MetricNamespace) {

  import management._

  def metrics: MetricMap = {
    val runtime = Runtime.getRuntime
    val maxMemory = runtime.maxMemory
    val allocatedMemory = runtime.totalMemory
    val freeMemory = runtime.freeMemory
    val memoryInfo: MetricMap = Map(
      (namespace.namespace / "system" / "memory") -> Map(
        (Map("type" -> "max")       -> maxMemory),
        (Map("type" -> "allocated") -> allocatedMemory),
        (Map("type" -> "free")      -> freeMemory)
      )
    )
    val gcInfo: MetricMap = {
      val beans = ManagementFactory.getGarbageCollectorMXBeans().toArray
      val (cycles, msec) = beans.foldLeft((ValueMap.Empty, ValueMap.Empty)){case ((cycles, msec), tastyBean: management.GarbageCollectorMXBean) =>
        val tags = Map("type" -> tastyBean.getName.replace(' ', '_'))
        (cycles + (tags -> tastyBean.getCollectionCount), msec + (tags -> tastyBean.getCollectionTime))
      }
      Map (
        (namespace.namespace / "system" / "gc" / "cycles") -> cycles,
        (namespace.namespace / "system" / "gc" / "msec") -> msec
      )
    }

    val fdInfo: MetricMap = ManagementFactory.getOperatingSystemMXBean match {
      case u: com.sun.management.UnixOperatingSystemMXBean => Map(
        (namespace.namespace / "system" / "fd_count") -> Map(Map() -> u.getOpenFileDescriptorCount)
      )
      case _ => MetricMap.Empty //for those poor souls using non-*nix
    }

    (memoryInfo ++ gcInfo ++ fdInfo).withTags(namespace.tags)
  }
}
