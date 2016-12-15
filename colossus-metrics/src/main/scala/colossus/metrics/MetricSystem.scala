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


/**
 * Configuration object for a [[MetricSystem]]
 *
 * @param enabled true to enable all functionality.  Setting to false will effectively create a dummy system that does nothing
 * @param name The name of the metric system.  Name is not used in the root path of a metric system.
 * @param collectionIntervals The intervals that the system should use to periodicaly collect metrics.  Multiple intervals can be specified to allow the collection of, for example, both per second and per minute metrics.
 * @param collectSystemMetrics whether to collect system metrics like GC usage
 * @param systemMetricsNamespace an optional namespace for system metrics, defaults to "/name" where name is the name of the metric system
 * @param collectorConfigs a typesafe config object containing configurations for individual collectors
 */
case class MetricSystemConfig(
  enabled: Boolean,
  name: String,
  systemMetrics: SystemMetricsConfig,
  collectorConfig: CollectorConfig
)

object MetricSystemConfig {

  val ConfigRoot = "colossus.metrics"

  /**
   * Load a MetricSystemConfig from a typesafe config object.  When providing a
   * config object, the format should be that of the `colossus.metrics` section
   * in the reference.conf.  For example, if creating a metric system named
   * "foo" that will create a rate named "bar", the config should look like:
   *
   * ```
   foo {
     system.enabled = true
     //...
   }
   bar.pruneEmpty = true
   ```
   */
  def load(name: String, config: Config = ConfigFactory.load().getConfig(ConfigRoot)): MetricSystemConfig = {
    val systemConfig = if (config.hasPath(name)) config.getConfig(name).withFallback(config) else config
    import ConfigHelpers._
    val enabled               = systemConfig.getBoolean("system.enabled")
    val collectSystemMetrics  = systemConfig.getBoolean("system.system-metrics.enabled")
    val metricIntervals       = systemConfig.getFiniteDurations("system.collection-intervals")
    val systemMetricsNamespace = {
      systemConfig.getStringOption("system.system-metrics.namespace").map{n =>
        if (n == "__NAME__") name else n
      }.getOrElse("/")
    }
    val collectorConfig = CollectorConfig(metricIntervals, config, systemConfig.getConfig("system.collector-defaults"))
    MetricSystemConfig(enabled, name, SystemMetricsConfig(collectSystemMetrics, systemMetricsNamespace) , collectorConfig)
  }

}

/**
 * The MetricSystem provides the environment for creating metrics and is required to create collectors.
 */
class MetricSystem private[metrics] (val config: MetricSystemConfig)(implicit system: ActorSystem) extends MetricNamespace {

  val namespace: MetricAddress = "/"

  private lazy val localHostname = try  {
    java.net.InetAddress.getLocalHost.getHostName
  } catch {
    case e: java.net.UnknownHostException => "unknown_host"
  }

  val tags: TagMap = Map("host" -> localHostname)

  protected val collection = new Collection(config.collectorConfig)


  private val intervalNamespace = if (config.systemMetrics.enabled) {
    Some(this / config.systemMetrics.namespace)
  } else {
    None
  }
  val collectionIntervals : Map[FiniteDuration, CollectionInterval] = config.collectorConfig.intervals.map{ interval =>
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
   * Create a new MetricSystem with the given configuration.  Be aware that if
   * creating multiple metric systems, `name` must be unique.
   *
   */
  def apply(config : MetricSystemConfig)(implicit system: ActorSystem): MetricSystem = {
    if (config.enabled) {
      new MetricSystem(config)
    } else {
      deadSystem(system)
    }
  }

  /**
   * Create a new MetricSystem with configuration automatically loaded from
   * location `colossus.metrics.<name>`
   */
  def apply(name: String)(implicit system: ActorSystem): MetricSystem = MetricSystem(MetricSystemConfig.load(name))

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
      systemMetrics = SystemMetricsConfig(false, "/"),
      collectorConfig = CollectorConfig(Nil, ConfigFactory.defaultReference().getConfig(MetricSystemConfig.ConfigRoot), ConfigFactory.defaultReference().getConfig(MetricSystemConfig.ConfigRoot + ".system.collector-defaults"))
    )
    new MetricSystem(deadconfig)
  }

}








