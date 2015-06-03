package colossus.metrics

import akka.actor._
import akka.agent.Agent

import scala.concurrent.duration._


class IntervalAggregator(namespace: MetricAddress, interval: FiniteDuration, snapshot: Agent[MetricMap], collectSystemMetrics: Boolean) extends Actor with ActorLogging {

  import context.dispatcher
  import IntervalAggregator._
  import java.util.{HashSet=>JHashSet}
  import scala.collection.JavaConversions._

  val systemMetrics = new SystemMetricsCollector(namespace)

  def blankMap(): MetricMap = if (collectSystemMetrics) systemMetrics.metrics else Map()

  var build: MetricMap = blankMap()
  //var metrics = systemMetrics.metrics
  val collectors = new JHashSet[ActorRef]()
  val reporters = new JHashSet[ActorRef]()
  var latestTick = 0L


  val metricSafeDurationName = interval.toString().replaceAll(" ", "") //ie: 1second, 44milliseconds
  val collectedGauge = new ConcreteGauge(Gauge(namespace / metricSafeDurationName / "metric_completion"))
  collectedGauge.set(0)

  //needs to be a float so that incrementCollected won't report 0s
  var tocksCollected : Float = 0
  var tocksExpected : Int = 0

  def receive = {

    case SendTick => {
      latestTick += 1
      collectors.foreach(_ ! Tick(latestTick, interval))
      finalizeAndReportMetrics()
      resetMetrics()
      context.system.scheduler.scheduleOnce(interval, self, SendTick)
    }

    case Tock(m, v) => {
      if (v == latestTick) {
        if(collectors.contains(sender())) {
          build = build <+> m
          incrementCollected()
        }else {
          log.warning(s"Received metrics from an unregistered EventCollector: ${sender()}")
        }
      }else{
        log.warning(s"Currently processing tick# $latestTick.  Received a tock message for an outdated tick#: $v.  Ignoring")
      }
    }

    case a @ RegisterCollector(ref) => registerComponent(a, ref, collectors)

    case a @ RegisterReporter(ref) => registerComponent(a, ref, reporters)

    case Terminated(child) => {
      if(collectors.contains(child)){
        log.debug(s"oh no!  We lost an EventCollector $child. Removing from registered collectors.")
        collectors.remove(child)
      }else if(reporters.contains(child)){
        log.debug(s"oh no!  We lost a MetricReporter $child. Removing from registered reporters.")
        reporters.remove(child)
      }else{
        log.warning(s"someone: $child died..for which there is no reporter or collector registered")
      }
    }

    case ListCollectors => {
      sender ! collectors.toSet //yea..that's right..immutable on the way out.
    }

    case ListReporters => {
      sender ! reporters.toSet //yea..that's right..immutable on the way out.
    }
  }

  private def incrementCollected() {
    tocksCollected += 1
    val pct = (tocksCollected / tocksExpected) * 100F
    collectedGauge.set(pct.toLong) //rounds down
  }

  private def finalizeAndReportMetrics() {
    val collectedMap = collectedGauge.metrics(CollectionContext(Map.empty, interval))
    build = build <+> collectedMap
    snapshot.send(build)
    reporters.foreach(_ ! ReportMetrics(build))
  }

  private def resetMetrics() {
    build = blankMap()
    collectedGauge.set(0L)
    tocksCollected = 0
    tocksExpected = collectors.size()
  }

  private def registerComponent(msg : Any, ref : ActorRef, refs : JHashSet[ActorRef]) {
    context.watch(ref)
    if(refs.contains(ref)){
      log.warning(s"Received ${msg.getClass.getCanonicalName} for $ref, which is already registered")
    }else{
      log.debug(s"Registered ${msg.getClass.getCanonicalName}: $ref")
      refs.add(ref)
    }
  }

  override def preStart() {
    context.system.scheduler.scheduleOnce(interval, self, SendTick)
  }
}

object IntervalAggregator {

  case class RegisterCollector(ref : ActorRef)
  case class RegisterReporter(ref : ActorRef)
  case class ReportMetrics(m : MetricMap)
  private[metrics] case object ListCollectors
  private[metrics] case object ListReporters
  private[metrics] case object SendTick

  private[metrics] case class Tick(value: Long, interval : FiniteDuration)
  private[metrics] case class Tock(metrics: MetricMap, tick: Long)

}


class SystemMetricsCollector(namespace: MetricAddress) {

  import management._
  import MetricValues._

  def metrics: MetricMap = {
    val runtime = Runtime.getRuntime
    val maxMemory = runtime.maxMemory
    val allocatedMemory = runtime.totalMemory
    val freeMemory = runtime.freeMemory
    val memoryInfo: MetricMap = Map(
      (namespace / "system" / "memory") -> Map(
        (Map("type" -> "max")       -> SumValue(maxMemory)),
        (Map("type" -> "allocated") -> SumValue(allocatedMemory)),
        (Map("type" -> "free")      -> SumValue(freeMemory))
      )
    )
    val gcInfo: MetricMap = ManagementFactory.getGarbageCollectorMXBeans().toArray.map{case tastyBean: management.GarbageCollectorMXBean =>
      val tags = Map("type" -> tastyBean.getName.replace(' ', '_'))
      Map(
        (namespace / "system" / "gc" / "cycles") -> Map(tags -> SumValue(tastyBean.getCollectionCount)),
        (namespace / "system" / "gc" / "msec") -> Map(tags -> SumValue(tastyBean.getCollectionTime))
      ) : MetricMap
    }.reduce{(a: MetricMap, b: MetricMap) => a <+> b}
    
    val fdInfo: MetricMap = ManagementFactory.getOperatingSystemMXBean match {    
      case u: com.sun.management.UnixOperatingSystemMXBean => Map(
        (namespace / "system" / "fd_count") -> Map(Map() -> SumValue(u.getOpenFileDescriptorCount))
      )
      case _ => MetricMap.Empty //for those poor souls using non-*nix
    }

    (memoryInfo <+> gcInfo <+> fdInfo)
  }
}
