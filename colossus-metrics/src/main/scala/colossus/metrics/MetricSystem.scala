package colossus.metrics

import akka.actor._
import akka.agent.Agent
import colossus.metrics.MetricAddress.Root
import com.typesafe.config.{ConfigFactory, Config}

import scala.concurrent.duration._

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
  def namespace: MetricAddress
  def collection : Collection


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
  * Metric Configuration
  *
  * A MetricSystem's configuration contains defaults for each metric type.  It can also contain configuration for individual metrics.
  * When a new Metric is created, a MetricSystem will first look for a metric entry corresponding to the MetricAddress of the metric.
  * If it finds one, it will use that entry overlaid on the default entry for that metric type to generate a complete configuration.
  * If it does not find one, it will fallback to just using the default entry for that metric type.
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
   * @param config Typesafe Config source which should contain configuration for a MetricSystem.  This Configuration object is used during
   *               Collector(Rate, Histogram specifically) creation.  If this Config does not have the corresponding colossus.metrics.collectors.defaults for those
   *               then Collector creation will fail if it tries to reference them.
   * @param system the actor system the metric system should use
   * @return
   */
  def apply(namespace: MetricAddress, collectionIntervals: Seq[FiniteDuration] = Seq(1.second, 1.minute),
            collectSystemMetrics: Boolean = true, config : Config = ConfigFactory.load())
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
    * @param system
    * @return
    */
  def deadSystem(implicit system: ActorSystem) = {
    MetricSystem(Root / "DEAD", Map[FiniteDuration, MetricInterval](), false, ConfigFactory.defaultReference())
  }

  /**
    * Create a new MetricSystem, using only the defaults provided by the corresponding "colossus.metrics" config path.
    * A Config object will be created via {{{ConfigFactory.load()}}}
    * @param system
    * @return
    */
  def apply()(implicit system : ActorSystem) : MetricSystem = {
    apply(ConfigRoot)
  }

  /**
    * Create a new MetricSystem using the supplied configPath.  This configPath will be overlaid on top of the default "colossus.metrics"
    * config path.
    * A Config object will be created via {{{ConfigFactory.load()}}}
    *
    * @param configPath  The path to the configuration.
    * @param system
    * @return
    */
  def apply(configPath : String)(implicit system : ActorSystem) : MetricSystem = {
    apply(configPath, ConfigFactory.load())
  }

  /**
    * Create a new MetricSystem by loading its config from the specified configPath.  This configPath will be overlaid on top of the default "colossus.metrics" config path.
    *
    * @param configPath The path to the configuration
    * @param config The Config source to query
    * @param system
    * @return
    */
  def apply(configPath : String, config : Config)(implicit system : ActorSystem) : MetricSystem = {

    import MetricSystemConfigHelpers._

    val userPathObject = config.getObject(configPath)
    val metricsObject = userPathObject.withFallback(config.getObject(ConfigRoot))
    val metricsConfig = metricsObject.toConfig

    //after creating the merged config object, overwrite colossus.metrics with this value and use that internally,
    //This simplifies Collector creation.
    val mergedConfig = config.withValue(ConfigRoot, metricsObject)

    val enabled = metricsConfig.getBoolean("enabled")
    if(enabled){
      val collectSystemMetrics = metricsConfig.getBoolean("collect-system-metrics")
      val metricIntervals = metricsConfig.getFiniteDurations("collection-intervals")
      val metricAddress = metricsConfig.getString("namespace")
      MetricSystem(MetricAddress(metricAddress), metricIntervals, collectSystemMetrics, mergedConfig)
    }else{
      deadSystem
    }
  }
}

//has to be a better way
object MetricSystemConfigHelpers {

  implicit class FiniteDurationLoader(config : Config) {

    import scala.collection.JavaConversions._

    def getFiniteDurations(path : String) : Seq[FiniteDuration] = config.getStringList(path).map(finiteDurationOnly(_, path))

    private def finiteDurationOnly(str : String, key : String) = {
      Duration(str) match {
        case duration : FiniteDuration => duration
        case other => throw new FiniteDurationExpectedException(s"$str is not a valid FiniteDuration.  Expecting only finite for path $key.  Evaluted to $other")
      }
    }
  }
}

private[metrics] class FiniteDurationExpectedException(str : String) extends Exception(str)






