package colossus.metrics

import akka.actor._
import akka.agent.Agent

import scala.concurrent.duration._


class MetricDatabase(systemId: MetricSystemId, namespace: MetricAddress, snapshot: Agent[MetricMap], collectSystemMetrics: Boolean) extends Actor with ActorLogging {
  import MetricClock._
  import MetricDatabase._

  val systemMetrics = new SystemMetricsCollector(namespace)

  def blankMap(): MetricMap = if (collectSystemMetrics) systemMetrics.metrics else Map()

  var latestTick = 0L
  var build: MetricMap = blankMap() 
  var metrics = systemMetrics.metrics

  def receive = {
    case Tick(id, v) if (id == systemId) => {
      latestTick = v
      metrics = build
      snapshot.alter(_ => metrics)
      build = blankMap()
    }
    case Tock(m, v) => if (v >= latestTick) {
      build = build << m
    }
    case GetDB => sender ! DB(metrics)
    case Query(filter) => sender ! QueryResult(metrics.filter(filter))

    case GetWindow(min, max) => {
      //this is now deprecated since we don't store more than 1 second of data ever
      sender ! Window(Frame(System.currentTimeMillis, metrics) :: Nil)
    }
  }

  override def preStart() {
    context.system.eventStream.subscribe(self, classOf[Tick])
  }

}
object MetricDatabase {
 case object GetDB
 case class Query(filter: MetricFilter)
 case class GetWindow(min: Option[Long], max: Option[Long])

 case class DB(metrics: MetricMap)
 case class QueryResult(metrics: MetricMap)

}


class MetricClock(systemId: MetricSystemId, period: FiniteDuration) extends Actor with ActorLogging {
  import context.dispatcher
  import MetricClock._
  
  override def preStart() {
    context.system.scheduler.schedule(period, period, self, SendTick)
  }

  var tickNum = 0L
  case object SendTick

  def receive = {
    case SendTick => {
      tickNum += 1
      context.system.eventStream.publish(Tick(systemId, tickNum))
    }
  }

}
object MetricClock {
  case class Tick(systemId: MetricSystemId, value: Long)
  case class Tock(metrics: MetricMap, tick: Long)
}





class SystemMetricsCollector(namespace: MetricAddress) {

  import management._

  def metrics: MetricMap = {
    val runtime = Runtime.getRuntime
    val maxMemory = runtime.maxMemory
    val allocatedMemory = runtime.totalMemory
    val freeMemory = runtime.freeMemory
    val memoryInfo: MetricMap = Map(
      (namespace / "system" / "memory") -> Map(
        (Map("type" -> "max")       -> maxMemory),
        (Map("type" -> "allocated") -> allocatedMemory),
        (Map("type" -> "free")      -> freeMemory)
      )
    )
    val gcInfo = ManagementFactory.getGarbageCollectorMXBeans().toArray.map{case tastyBean: management.GarbageCollectorMXBean =>
      val tags = Map("type" -> tastyBean.getName.replace(' ', '_'))
      Map(
        (namespace / "system" / "gc" / "cycles") -> Map(tags -> tastyBean.getCollectionCount),
        (namespace / "system" / "gc" / "msec") -> Map(tags -> tastyBean.getCollectionTime)
      )
    }.reduce{_ << _}
    
    val fdInfo: MetricMap = ManagementFactory.getOperatingSystemMXBean match {    
      case u: com.sun.management.UnixOperatingSystemMXBean => Map(
        (namespace / "system" / "fd_count") -> Map(Map() -> u.getOpenFileDescriptorCount)
      )
      case _ => MetricMap.Empty //for those poor souls using non-*nix
    }

    (memoryInfo << gcInfo << fdInfo)
  }

}


class TickTracker(period: FiniteDuration) {
  import TickTracker._

  var tickAccum = 0.seconds

  def tick(amount: FiniteDuration): TickResult = {
    tickAccum += amount
    if (tickAccum >= period) {
      tickAccum -= period
      Tick
    } else {
      NoTick
    }
  }
}

object TickTracker {
  sealed trait TickResult
  case object Tick extends TickResult
  case object NoTick extends TickResult
}
