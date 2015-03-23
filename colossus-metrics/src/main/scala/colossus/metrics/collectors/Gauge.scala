package colossus.metrics

import akka.actor._
import EventLocality._

import scala.concurrent.duration._

trait Gauge extends EventCollector {
  def set(value: Long, tags: TagMap = TagMap.Empty) {
    set(Some(value), tags)
  }
  def set(value: Option[Long], tags: TagMap)

  
}

case class GaugeParams(address: MetricAddress, expireAfter: Duration = 1.hour, expireTo: Option[Long] = None) extends MetricParams[Gauge, GaugeParams] {
  def transformAddress(f: MetricAddress => MetricAddress) = copy(address = f(address))

}

object Gauge {
  
  case class SetValue(address: MetricAddress, value: Option[Long], tags: TagMap) extends MetricEvent

  def apply(address: MetricAddress, expireAfter: Duration = 1.hour, expireTo: Option[Long] = None) = GaugeParams(address, expireAfter, expireTo)
  
  implicit object GaugeGenerator extends Generator[Gauge, GaugeParams] {
    def local(params: GaugeParams): Local[Gauge] = new ConcreteGauge(params)
    def shared(params: GaugeParams)(implicit collector: ActorRef): Shared[Gauge] = new SharedGauge(params, collector)
  }

}

class BasicGauge(params: GaugeParams)  {
  import params._

  var value: Option[Long] = None

  private var timeSinceLastSet: FiniteDuration = 100000.hours

  def set(newValue: Option[Long]) {
    value = newValue
    timeSinceLastSet = 0.seconds
  }

  def tick(period: FiniteDuration) {
    timeSinceLastSet += period
    if (timeSinceLastSet > expireAfter) {
      value = expireTo
    }
  }


}


class ConcreteGauge(params: GaugeParams) extends Gauge with LocalLocality with TickedCollector {
  val address = params.address

  val gauges = collection.mutable.Map[TagMap, BasicGauge]()

  def set(newValue: Option[Long], tags: TagMap) {
    if (!gauges.contains(tags)) {
      gauges(tags) = new BasicGauge(params)
    }
    gauges(tags).set(newValue)
  }

  def tick(period: FiniteDuration) {
    gauges.foreach{case (tags, gauge) => 
      gauge.tick(period)
      if (!gauge.value.isDefined) {
        gauges -= tags
      }
    }
  }

  def metrics(context: CollectionContext): MetricMap = {
    Map(params.address -> gauges.flatMap{case (tags, gauge) => gauge.value.map{v => tags -> v}}.toMap)
  }

  def event: PartialFunction[MetricEvent, Unit] = {
    case Gauge.SetValue(_, v, t) => set(v,t)
  }

}

class SharedGauge(params: GaugeParams, collector: ActorRef) extends Gauge with SharedLocality {
  val address = params.address

  def set(newValue: Option[Long], tags: TagMap) {
    collector ! Gauge.SetValue(address, newValue, tags)
  }
}
