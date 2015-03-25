package colossus.metrics

import akka.actor._

import scala.concurrent.duration._

import java.util.concurrent.ConcurrentHashMap

import scala.language.higherKinds
import scala.reflect.ClassTag

/**
 * The base trait for any exported value from an event collector
 *
 * This requires that every matric value is a semi-group (associative +
 * operation), however they should really be monoids (semi-group with a zero
 * value).  Unfortunately this cannot be enforced by this trait since these are
 * passed in actor messages and must be monomorphic
 */
trait MetricValue {
  def + (b: MetricValue): Option[MetricValue]

  def toRawMetrics(address: MetricAddress, globalTags: TagMap): RawMetricMap
}



trait MetricEvent {
  def address: MetricAddress
}

case class MetricSystemConfig(tickPeriod: FiniteDuration = 1.second)

sealed trait Locality
trait SharedLocality extends Locality
trait LocalLocality  extends Locality with MetricProducer {
  def event: PartialFunction[MetricEvent, Unit]
}
  

trait EventLocality[E <: EventCollector]
object EventLocality {
  type Shared[E <: EventCollector] = E with SharedLocality
  type Local[E <: EventCollector] = E with LocalLocality
}
import EventLocality._

//TOdO: need to eliminate tag duplication
case class CollectionContext(globalTags: TagMap)

trait EventCollector {
  def address: MetricAddress
}

/**
 * Since we are basically requiring every EventCollector to be constructed with
 * a single parameter (which should be a case class), this trait is required to
 * be extended by that parameter.  The type parameter exists solely for the
 * purpose of type inference, so we can do getOrAdd(Rate(...)), and simply by
 * supplying the Rate (which returns a RateParams, which extends
 * MetricParams[Rate]), the method can infer that it should be returning a Rate
 */
trait MetricParams[E <: EventCollector, T] {self: T =>
  //not sure if this is needed
  val address: MetricAddress

  //we need the f-bound polymorphism to retain the transformed type
  def transformAddress(f: MetricAddress => MetricAddress): T
}

trait TickedCollector extends EventCollector {
  def tick(tickPeriod: FiniteDuration)
}

/**
 * This is a typeclass that is basically just an EventCollector factory.
 *
 */
trait Generator[C <: EventCollector, P] {
  //notice - we "should" restrict the P type to be a subtype of MetricParams[T],
  //however that makes this incompatible with the type parameters of the
  //collection's getOrAdd method.  And we are not allow to add view-bounds to a
  //trait, so for now we're kind of stuck


  /**
   * Generate a local collector given the params object
   */
  def local(params: P): C with LocalLocality

  /**
   * Generate a shared collector given the params object
   */
  def shared(params:P)(implicit collector: ActorRef): C with SharedLocality
}



trait Collection[L <: Locality] {
  import Collection.ParamsFor
  //notice - the type lambda is basically the same as a view bounds (P <%
  //MetricParams[U]), but view bounds are deprecated.  We need this instead of
  //just subtype (P <: MetricParams[U]) becuase for some reason type inference
  //fails when we do that
  def getOrAdd[T <: EventCollector : ClassTag, P : ParamsFor[T]#Type](params: P)(implicit generator: Generator[T,P]): T with L

}
object Collection {
  type ParamsFor[U <: EventCollector] = ({type Type[P] = P => MetricParams[U,P]})
}


class DuplicateMetricException(message: String) extends Exception(message)

/**
 * A Local collection is designed only to be used inside an actor or otherwise
 * thread-safe environment.  Notice that even though the collection we're
 * storing the event collectors is thread-safe, the collectors themselves are
 * not.  The collector actor must be the actor receiving events for metrics in
 * the map
 *
 * note - this is not intended to be directly constructed by users
 */
class LocalCollection(
  namespace: MetricAddress = MetricAddress.Root,
  val globalTags: TagMap = TagMap.Empty, 
  metrics:ConcurrentHashMap[MetricAddress, Local[EventCollector]] = new ConcurrentHashMap[MetricAddress, Local[EventCollector]],
  parent: Option[LocalCollection] = None
) extends Collection[LocalLocality] {
  import Collection.ParamsFor
  import LocalCollection._

  val localProducers = collection.mutable.ArrayBuffer[MetricProducer]()


  def getOrAdd[T <: EventCollector : ClassTag, P : ParamsFor[T]#Type](_params: P)(implicit creator: Generator[T, P]): Local[T] = {
    val params: P = _params.transformAddress(namespace / _)
    parent.map{_.getOrAdd(params)}.getOrElse {
      val created = creator.local(params)
      metrics.putIfAbsent(params.address, created: Local[EventCollector]) match {
        case null => created
        case exists: T => {
          //because of type erasure, if we try to match against
          //Local[T] or T with LocalLocality[T], it creates false positives (it
          //thinks a Local[Counter] is the same type as a Local[Rate]), bad :( 
          exists.asInstanceOf[Local[T]] 
        }
        case other => throw new DuplicateMetricException(s"An event collector of type ${other.getClass.getSimpleName} already exists")
      }
    }
  }

  def handleEvent(event: MetricEvent): EventResult = {
    Option(metrics.get(event.address)).map{c =>
      c.event.andThen{_ => Ok}.orElse[MetricEvent,EventResult]({
        case _ => InvalidEvent
      })(event)
    }.getOrElse{
      UnknownMetric
    }

  }

  def attach(producer: MetricProducer) {
    localProducers += producer
  }

  def aggregate: MetricMap = {
    //do not move this import unless you want to have a bad time
    import scala.collection.JavaConversions._
    val now = System.currentTimeMillis
    val context = CollectionContext(globalTags)
    val eventMetrics = metrics.values.map{_.metrics(context)}
    val producerMetrics = localProducers.map{_.metrics(context)}
    (eventMetrics ++ producerMetrics)
      .foldLeft[MetricMap](Map()){case (build, m) => build << m}
      .filter{! _._2.isEmpty}
  }

  def tick(tickPeriod: FiniteDuration) {
    import scala.collection.JavaConversions._
    metrics.values.collect{
      case t: TickedCollector => t.tick(tickPeriod)
    }
  }

  def subCollection(subSpace: MetricAddress = MetricAddress.Root, subTags: TagMap = TagMap.Empty) = {
    new LocalCollection(subSpace, subTags, metrics, Some(this))
  }

}

object LocalCollection {
  sealed trait EventResult
  sealed trait EventError extends EventResult

  case object InvalidEvent extends EventError
  case object UnknownMetric extends EventError
  case object Ok extends EventResult
}


/**
 * A Shared collection is a collection where every metric it returns is
 * completely thread-safe.  A Shared collection is useful if you want to pass
 * one collection to many actors, but be aware that since every event is sent
 * as a separate actor message (as opposed to local collections which collect
 * events just as function calls), this should not be used for very-high
 * frequency events.
 */
private[colossus] class SharedCollection(val local: LocalCollection, val collector: ActorRef) extends Collection[SharedLocality] {

  import Collection.ParamsFor

  def getOrAdd[T <: EventCollector : ClassTag, P : ParamsFor[T]#Type](params: P)(implicit creator: Generator[T, P]): Shared[T] = {
    //create the actual collector
    local.getOrAdd(params)
    //get a shared interface for it
    creator.shared(params)(collector)
  }

  def shared = this
}

object SharedCollection {

  def apply(globalTags: TagMap = TagMap.Empty)(implicit system: MetricSystem, fact: ActorRefFactory): SharedCollection = {
    
    val metrics = new ConcurrentHashMap[MetricAddress, Local[EventCollector]]
    val local = new LocalCollection(MetricAddress.Root, globalTags, metrics)

    val collector = fact.actorOf(Props(classOf[Collector], system, local))
    
    new SharedCollection(local, collector)
  }

}
