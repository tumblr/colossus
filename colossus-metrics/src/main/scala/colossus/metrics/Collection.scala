package colossus.metrics

import akka.actor._

import scala.concurrent.duration._

import java.util.concurrent.ConcurrentHashMap

import scala.language.higherKinds
import scala.reflect.ClassTag

trait MetricEvent {
  def address: MetricAddress
}

case class MetricSystemConfig(tickPeriod: FiniteDuration = 1.second)

sealed trait Locality[+T] {
  def shared : T with SharedLocality[T]
}
trait SharedLocality[+T] extends Locality[T] {self: T =>
  def shared : T with SharedLocality[T] = self
}
trait LocalLocality[+T] extends Locality[T] with MetricProducer {
  def event: PartialFunction[MetricEvent, Unit]
}
  

trait EventLocality[E <: EventCollector]
object EventLocality {
  type Shared[E <: EventCollector] = E with SharedLocality[E]
  type Local[E <: EventCollector] = E with LocalLocality[E]
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
 * Note - we may be able to get rid of the Locality type-param since as of now
 * it looks like we only need generators for local collectors, but we'll leave
 * it for now since it's fully hidden from users anyway
 */
trait Generator[L[_] <: Locality[_], C <: EventCollector, P] {
  //notice - we "should" restrict the P type to be a subtype of MetricParams[T],
  //however that makes this incompatible with the type parameters of the
  //collection's getOrAdd method.  And we are not allow to add view-bounds to a
  //trait, so for now we're kind of stuck


  def apply(params: P)(implicit collector: ActorRef): C with L[C]
}



trait Collection[L[_] <: Locality[_]] {
  import Collection.ParamsFor
  //notice - the type lambda is basically the same as a view bounds (P <%
  //MetricParams[U]), but view bounds are deprecated.  We need this instead of
  //just subtype (P <: MetricParams[U]) becuase for some reason type inference
  //fails when we do that
  def getOrAdd[T <: EventCollector : ClassTag, P : ParamsFor[T]#Type](params: P)(implicit generator: Generator[LocalLocality,T,P]): T with L[T]

  def shared: Collection[SharedLocality]

  //def get[U <: EventCollector](address: MetricAddress)
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
  globalTags: TagMap = TagMap.Empty, 
  metrics:ConcurrentHashMap[MetricAddress, Local[EventCollector]] = new ConcurrentHashMap[MetricAddress, Local[EventCollector]],
  parent: Option[LocalCollection] = None
)
(implicit val collector: ActorRef) extends Collection[LocalLocality] {
  import Collection.ParamsFor
  import LocalCollection._

  val localProducers = collection.mutable.ArrayBuffer[MetricProducer]()


  def getOrAdd[T <: EventCollector : ClassTag, P : ParamsFor[T]#Type](_params: P)(implicit creator: Generator[LocalLocality,T, P]): Local[T] = {
    val params: P = _params.transformAddress(namespace / _)
    parent.map{_.getOrAdd(params)}.getOrElse {
      val created = creator(params)
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
    new LocalCollection(subSpace, subTags, metrics, Some(this))(collector)
  }

  def shared: SharedCollection = new SharedCollection(this)
    
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
class SharedCollection(val local: LocalCollection) extends Collection[SharedLocality] {

  import Collection.ParamsFor

  def getOrAdd[T <: EventCollector : ClassTag, P : ParamsFor[T]#Type](params: P)(implicit creator: Generator[LocalLocality,T, P]): Shared[T] = local.getOrAdd(params).shared

  def shared = this
}

object SharedCollection {

  def apply(globalTags: TagMap = TagMap.Empty)(implicit system: MetricSystem, fact: ActorRefFactory): SharedCollection = {
    
    val metrics = new ConcurrentHashMap[MetricAddress, Local[EventCollector]]

    val collector = fact.actorOf(Props(classOf[Collector], system, metrics, globalTags))
    
    //this is kind of weird, since we're effectively creating two local
    //collections.  Notice we can't yet pass a local collection to the actor
    //since it needs the actorref to be created.  we should create some kind of
    //base collection that has the getOrAdd method, but not a huge deal
    val local = new LocalCollection(MetricAddress.Root, globalTags, metrics)(collector)
    new SharedCollection(local)
  }

}


//this is a simplified model of the type system for metrics, keeping this here for experimenting
private[metrics] object CollectionModel {
  trait Obj

  trait GenParam
  trait Param[O <: Obj, R] extends GenParam {self: R =>
    def transform: R
  }

  type ParamsFor[U <: Obj] = ({type Type[P] = P => Param[U,P]})

  case class A(a: Int) extends Obj
  case class B(a: Int) extends Obj
  case class P(a: Int) extends Param[A,P]{
    def transform: P = P(a + 1)
  }


  trait Gen[O <: Obj, P] {
    def apply(p: P): O
  }
  implicit object AGen extends Gen[A,P] {
    def apply(p: P) = A(p.a)
  }

  def foo[T <: Obj : ClassTag, P : ParamsFor[T]#Type](p: P)(implicit g: Gen[T,P]): T = {
    g(p.transform)
  }

  val x: A = foo(P(4))
  //val y: B = foo(P(3))
}

