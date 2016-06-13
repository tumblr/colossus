package colossus.metrics

import akka.actor._
import akka.agent.Agent
import colossus.metrics.MetricAddress.Root
import com.typesafe.config.{ConfigFactory, Config}

import scala.concurrent.duration._
import scala.reflect.ClassTag

class CollectionInterval private[metrics](
  name : String,
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
  def report(config : MetricReporterConfig)(implicit fact: ActorRefFactory) : ActorRef = MetricReporter(config, intervalAggregator, name)
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

case class SystemMetricsConfig(enabled: Boolean, namespace: MetricAddress)

case class MetricSystemConfig(
  enabled: Boolean,
  name: String,
  collectionIntervals: Seq[FiniteDuration],
  systemMetricsConfig: SystemMetricsConfig,
  collectorConfigs: Config
)

object MetricSystemConfig {

  val ConfigRoot = "colossus.metrics"
  
  def load(config: Config = ConfigFactory.load().getConfig(ConfigRoot)): MetricSystemConfig = {

    import ConfigHelpers._
    val enabled = config.getBoolean("system.enabled")
    val name = config.getString("system.name")
    val collectSystemMetrics = config.getBoolean("system.system-metrics.enabled")
    val systemMetricsNamespace = config.getString("system.system-metrics.namespace")
    val metricIntervals = config.getFiniteDurations("system.collection-intervals")
    MetricSystemConfig(enabled, name, metricIntervals, SystemMetricsConfig(collectSystemMetrics, systemMetricsNamespace), config)
  }

}

/**
  * The MetricSystem is a set of actors which handle the background operations of dealing with metrics. In most cases,
  * you only want to have one MetricSystem per application.
  *
  * The currently provided metric types are [[colossus.metrics.Rate]], [[colossus.metrics.Histogram]] and [[colossus.metrics.Counter]].
  * New metric types can be created by implementing the [[colossus.metrics.Collector]] trait.
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
  * @param systemMetricsConfig configuration for built-in system metrics
  * @param config Config object from which Metric configurations will be sourced.
 */
class MetricSystem private[metrics] (val config: MetricSystemConfig)(implicit system: ActorSystem) extends MetricNamespace {

  val namespace: MetricAddress = "/"

  private val localHostname = java.net.InetAddress.getLocalHost.getHostName
  val tags: TagMap = Map("host" -> localHostname)

  protected val collection = new Collection(CollectorConfig(config.collectionIntervals, config.collectorConfigs))


  private val intervalNamespace = if (config.systemMetricsConfig.enabled) {
    Some(this / config.systemMetricsConfig.namespace)
  } else {
    None
  }
  val collectionIntervals : Map[FiniteDuration, CollectionInterval] = config.collectionIntervals.map{ interval =>
    import system.dispatcher
    val snap = Agent[MetricMap](Map())
    val aggregator : ActorRef = system.actorOf(Props(classOf[IntervalAggregator], interval, snap, intervalNamespace))
    val i = new CollectionInterval(config.name, interval, aggregator, snap)
    interval -> i
  }.toMap

  //TODO : since we only have a single collection per metric system now, there's no need to register it like this
  collectionIntervals.values.foreach(_.intervalAggregator ! IntervalAggregator.RegisterCollection(collection))

}

/**
  * Factory for [[colossus.metrics.MetricSystem]] instances
  */
object MetricSystem {


  /**
   * Constructs a metric system
   *
   */
  def apply(config : MetricSystemConfig = MetricSystemConfig.load())(implicit system: ActorSystem): MetricSystem = {
    if (config.enabled) {
      new MetricSystem(config)
    } else {
      deadSystem(system)
    }
  }

  /**
    * Create a system which does nothing.  Useful for testing/debugging.
    *
    * @param system
    * @return
    */
  def deadSystem(implicit system: ActorSystem) = {
    val deadconfig = MetricSystemConfig(
      enabled = false,
      name = "DEAD",
      collectionIntervals = Nil,
      systemMetricsConfig = SystemMetricsConfig(false, "/"),
      collectorConfigs =ConfigFactory.defaultReference().getConfig(MetricSystemConfig.ConfigRoot)
    )
    new MetricSystem(deadconfig)
  }

}








