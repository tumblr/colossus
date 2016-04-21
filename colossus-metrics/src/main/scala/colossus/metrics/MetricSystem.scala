package colossus.metrics

import akka.actor._
import akka.agent.Agent
import colossus.metrics.MetricAddress.Root
import com.typesafe.config.{ConfigFactory, Config}

import scala.concurrent.duration._
import scala.reflect.ClassTag

class MetricInterval private[metrics](val namespace : MetricAddress,
                                      val interval : FiniteDuration,
                                      val intervalAggregator : ActorRef, snapshot : Agent[MetricMap]){

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
 * A MetricNamespace is essentially just an address prefix and is needed when
 * getting or creating collectors.  The `namespace` address is prefixed onto the
 * given address for the collector to create the full address.
 */
trait MetricNamespace {

  /**
    * The root of this MetricNamespace.  All Metrics added to this namespace will be made to be relative to this address.
    * @return
    */
  def namespace: MetricAddress


  protected def collection : Collection

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
    collection.getOrAdd(fullAddress)(f)
  }

  /**
   * Get a new namespace by appending `subpath` to the namespace
   */
  def /(subpath: MetricAddress): MetricNamespace = MetricContext(namespace / subpath, collection)

}

case class MetricContext(namespace: MetricAddress, collection: Collection) extends MetricNamespace

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
case class MetricSystem private[metrics] (namespace: MetricAddress, collectionIntervals : Map[FiniteDuration, MetricInterval],
                                          collectionSystemMetrics : Boolean, config : Config) extends MetricNamespace {

  val collection = new Collection(CollectorConfig(collectionIntervals.keys.toSeq, config))
  registerCollection(collection)

  protected def registerCollection(collection: Collection): Unit = {
    collectionIntervals.values.foreach(_.intervalAggregator ! IntervalAggregator.RegisterCollection(collection))
  }

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


    val m : Map[FiniteDuration, MetricInterval] = collectionIntervals.map{ interval =>
      val snap = Agent[MetricMap](Map())
      val aggregator : ActorRef = createIntervalAggregator(system, namespace, interval, snap, collectSystemMetrics)
      val i = new MetricInterval(namespace, interval, aggregator, snap)
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
    MetricSystem(Root / "DEAD", Map[FiniteDuration, MetricInterval](), false, ConfigFactory.defaultReference().getConfig(MetricSystem.ConfigRoot))
  }

  /**
    * Create a new MetricSystem, using the specified configuration
    *
    * @param config Config object expected to be in the shape of the reference.conf's `colossus.metrics` definition.
    * @param system
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

//has to be a better way
object ConfigHelpers {

  implicit class ConfigExtractors(config : Config) {

    import scala.collection.JavaConversions._

    def getStringOption(path : String) : Option[String] = getOption(path, config.getString)

    def getIntOption(path : String) : Option[Int] = getOption(path, config.getInt)

    def getLongOption(path : String) : Option[Long] = getOption(path, config.getLong)

    private def getOption[T](path : String, f : String => T) : Option[T] = {
      if(config.hasPath(path)){
        Some(f(path))
      }else{
        None
      }
    }

    def getFiniteDurations(path : String) : Seq[FiniteDuration] = config.getStringList(path).map(finiteDurationOnly(_, path))

    def getFiniteDuration(path : String) : FiniteDuration = finiteDurationOnly(config.getString(path), path)

    def getScalaDuration(path : String) : Duration =  Duration(config.getString(path))

    private def finiteDurationOnly(str : String, key : String) = {
      Duration(str) match {
        case duration : FiniteDuration => duration
        case other => throw new FiniteDurationExpectedException(s"$str is not a valid FiniteDuration.  Expecting only finite for path $key.  Evaluted to $other")
      }
    }

    def withFallbacks(paths : String*) : Config = {
      //starting from empty, walk back from the lowest priority, stacking higher priorities on top of it.
      paths.reverse.foldLeft(ConfigFactory.empty()) {
        case (acc, path) =>if(config.hasPath(path)){
          config.getConfig(path).withFallback(acc)
        } else{
          acc
        }
      }
    }
  }
}

private[metrics] class FiniteDurationExpectedException(str : String) extends Exception(str)






