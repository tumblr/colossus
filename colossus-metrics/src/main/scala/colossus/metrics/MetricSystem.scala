package colossus.metrics

import akka.actor._
import akka.agent.Agent
import colossus.metrics.MetricAddress.Root
import com.typesafe.config.{ConfigFactory, Config}

import scala.concurrent.duration._
import scala.reflect.ClassTag

class CollectionInterval private[metrics](
  namespace : MetricAddress,
  interval : FiniteDuration,
  private[colossus] val intervalAggregator : ActorRef,
  snapshot : Agent[MetricMap]
){

  /**
   * The latest metrics snapshot
    *
    * @return
   */
  def last : MetricMap = snapshot.get()

  /**
   * Attach a reporter to this MetricPeriod.
    *
    * @param config  The [[MetricReporterConfig]] used to configure the [[MetricReporter]]
   * @return
   */
  def report(config : MetricReporterConfig)(implicit fact: ActorRefFactory) : ActorRef = MetricReporter(config, intervalAggregator, namespace)
}


/**
 * A MetricNamespace is essentially just an address prefix and set of tags. It is needed when
 * getting or creating collectors. The `namespace` address is prefixed onto the
 * given address for the collector to create the full address. Tags are added to each collector under under this
 * context.
 *
 * {{{
 * val subnameSpace: MetricContext = namespace / "foo" * ("a" -> "b")
 * }}}
 */
trait MetricNamespace {

  /**
    * The root of this MetricNamespace.  All Metrics added to this namespace will be made to be relative to this address.
    * @return
    */
  def namespace: MetricAddress

  /**
    * Tags applied to all collectors in this namespace
    * @return
    */
  def tags: TagMap

  protected def collection : Collection

  /**
    * Add tags to context
    * @param tags TagMap
    * @return Returns a new context with supplied tags added
    */
  def *(tags: (String, String)*): MetricContext = MetricContext(namespace, collection, this.tags ++ tags)

  def withTags(tags: (String, String)*): MetricContext = *(tags:_*)

  /**
    * Retrieve a [[Collector]] of a specific type by address, creating a new one if
    * it does not exist.  If an existing collector of a different type already
    * exists, a `DuplicateMetricException` will be thrown.
    * @param address Address meant to be relative to this MetricNamespace's namespace
    * @param f Function which takes in an absolutely pathed MetricAddress, and a [[CollectorConfig]] and returns an instance of a [[Collector]]
    * @tparam T
    * @return  A newly created instance of [[Collector]], created by `f` or an existing [[Collector]] if one already exists with the same MetricAddress
    */
  def getOrAdd[T <: Collector : ClassTag](address : MetricAddress)(f : (MetricAddress, CollectorConfig) => T) : T = {
    val fullAddress =  namespace / address
    collection.getOrAdd(fullAddress, tags)(f)
  }

  /**
   * Get a new namespace by appending `subpath` to the namespace
   */
  def /(subpath: MetricAddress): MetricContext = MetricContext(namespace / subpath, collection, tags)

}

case class MetricContext(namespace: MetricAddress, collection: Collection, tags: TagMap = TagMap.Empty) extends MetricNamespace

/**
  * The MetricSystem is a set of actors which handle the background operations of dealing with metrics. In most cases,
  * you only want to have one MetricSystem per application.
  *
  * The currently provided metric types are [[colossus.metrics.Rate]], [[colossus.metrics.Histogram]] and [[colossus.metrics.Counter]].
  * New metric types can be created by implementing the [[colossus.metrics.Collector]] trait.
  *
  * Namespace is the root of this MetricSystem's addressing.  All metrics which are registered within this MetricSystem will have their
  * addresses prefixed with this value.
  *
  * Metrics are collected and reported for each collectionInterval specified.
  *
  * A MetricSystem's configuration contains defaults for each metric type.  It can also contain configuration for additional metric definitions
  *
  * Metric Creation & Configuration
  *
  * All Metrics have 3 constructors.  Using [[colossus.metrics.Rate]] as an example:
  *
  *  - Rate(MetricAddress) => This will create a Rate with the MetricAddress, and use MetricSystem definition's default Rate configuration
  *                           Config precedence is as follow: `colossus.metrics.$MetricAddress`, `colossus.metrics.system.default-collectors.rate`
  *  - Rate(MetricAddress, configPath) => This will create a Rate with the MetricAddress.  configPath is relative to the MetricSystem's definition root.
  *                                       Config precedence is as follow: `colossus.metrics.$MetricAddress`, `colossus.metrics.$configPath`, `colossus.metrics.system.default-collectors.rate`
  *  - Rate(parameters) => Bypasses config, and creates the Rate directly with the passed in parameters
  *
  * Metric Disabling
  * There are 2 ways to disable a Metric:
  *  - set 'enabled : false' in its configuration. This will affect any Rate using that definition
  *  - Directly at the construction site, set enabled = false
  *
  * @param namespace Base url for all metrics within this MetricSystem
  * @param collectionIntervals Intervals for which this MetricSystem reports its data.
  * @param config Config object from which Metric configurations will be sourced.
 */
case class MetricSystem private[metrics] (namespace: MetricAddress, collectionIntervals : Map[FiniteDuration, CollectionInterval],
                                          collectionSystemMetrics : Boolean, config : Config) extends MetricNamespace {

  private val localHostname = java.net.InetAddress.getLocalHost.getHostName
  val tags: TagMap = Map("host" -> localHostname)

  protected val collection = new Collection(CollectorConfig(collectionIntervals.keys.toSeq, config))
  collectionIntervals.values.foreach(_.intervalAggregator ! IntervalAggregator.RegisterCollection(collection))

}

/**
  * Factory for [[colossus.metrics.MetricSystem]] instances
  */
object MetricSystem {

  val ConfigRoot = "colossus.metrics"

  /**
   * Constructs a metric system
   *
   * @param namespace Base url for all metrics within this MetricSystem
   * @param collectionIntervals How often to report metrics
   * @param collectSystemMetrics whether to collect metrics from the system as well
   * @param config Config object expected to be in the shape of the reference.conf's `colossus.metrics` definition.
   * @param system the actor system the metric system should use
   * @return
   */
  def apply(namespace: MetricAddress, collectionIntervals: Seq[FiniteDuration],
            collectSystemMetrics: Boolean, config : Config)
  (implicit system: ActorSystem): MetricSystem = {
    import system.dispatcher


    val m : Map[FiniteDuration, CollectionInterval] = collectionIntervals.map{ interval =>
      val snap = Agent[MetricMap](Map())
      val aggregator : ActorRef = createIntervalAggregator(system, namespace, interval, snap, collectSystemMetrics)
      val i = new CollectionInterval(namespace, interval, aggregator, snap)
      interval -> i
    }.toMap

    MetricSystem(namespace, m, collectSystemMetrics, config)
  }

  private def createIntervalAggregator(system : ActorSystem, namespace : MetricAddress, interval : FiniteDuration,
                                       snap : Agent[MetricMap], collectSystemMetrics : Boolean) = {
    system.actorOf(Props(classOf[IntervalAggregator], namespace, interval, snap, collectSystemMetrics))
  }

  /**
    * Create a system which does nothing.  Useful for testing/debugging.
    *
    * @param system
    * @return
    */
  def deadSystem(implicit system: ActorSystem) = {
    MetricSystem(Root / "DEAD", Map[FiniteDuration, CollectionInterval](), false, ConfigFactory.defaultReference().getConfig(MetricSystem.ConfigRoot))
  }

  /**
    * Create a new MetricSystem, using the specified configuration
    *
    * @param config Config object expected to be in the shape of the reference.conf's `colossus.metrics` definition.
    * @return
    */
  def apply(config : Config = loadDefaultConfig())(implicit system : ActorSystem) : MetricSystem = {

    import ConfigHelpers._

    val enabled = config.getBoolean("system.enabled")
    if(enabled){
      val collectSystemMetrics = config.getBoolean("system.collect-system-metrics")
      val metricIntervals = config.getFiniteDurations("system.collection-intervals")
      val metricAddress = config.getString("system.namespace")
      MetricSystem(MetricAddress(metricAddress), metricIntervals, collectSystemMetrics, config)
    }else{
      deadSystem
    }
  }
  private def loadDefaultConfig() = ConfigFactory.load().getConfig(ConfigRoot)
}







