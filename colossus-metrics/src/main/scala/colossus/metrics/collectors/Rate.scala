package colossus.metrics

import com.typesafe.config.Config

import scala.concurrent.duration._

/**
  * Metrics Collector which increments a value and resets after collection.
  */
trait Rate extends Collector{

  /**
    * Increment value to this Rate for this specified TagMap
    * A single Rate instance divides values amongst TagMaps, and tracks each one independently
    * When they are collected and reported, all TagMaps will be reported under the same MetricAddress.
    *
    * @param tags Tags to record with this value
    * @param amount The value to increment the rate
    */
  def hit(tags : TagMap = TagMap.Empty, amount : Long = 1)

  /**
    * Instruct the collector to not report any values for tag combinations which were previously empty.
    *
    * @return
    */
  def pruneEmpty : Boolean

}

//working implementation of a Rate
class DefaultRate private[metrics](val address: MetricAddress, val pruneEmpty: Boolean, intervals : Seq[FiniteDuration])extends Rate {

  private val maps: Map[FiniteDuration, CollectionMap[TagMap]] = intervals.map{ i => (i, new CollectionMap[TagMap])}.toMap

  //note - the totals are shared amongst all intervals, and we use the smallest
  //interval to update them
  private val totals = new CollectionMap[TagMap]
  private val minInterval = if (intervals.size > 0) intervals.min else Duration.Inf

  def hit(tags: TagMap = TagMap.Empty, amount: Long = 1) {
    maps.foreach{ case (_, map) => map.increment(tags) }
  }

  def tick(interval: FiniteDuration): MetricMap  = {
    val snap = maps(interval).snapshot(pruneEmpty, true)
    if (interval == minInterval) {
      snap.foreach{ case (tags, value) => totals.increment(tags, value) }
    }
    if (snap.isEmpty) Map() else {
      Map(address -> snap, address / "count" -> totals.snapshot(pruneEmpty, false))
    }
  }
}

//Dummy implementation of a Rate, used when "enabled=false" is specified at creation
class NopRate private[metrics](val address : MetricAddress, val pruneEmpty : Boolean)extends Rate {

  private val empty : MetricMap = Map()

  override def tick(interval: FiniteDuration): MetricMap = empty

  override def hit(tags: TagMap, value: MetricValue): Unit = {}
}

object Rate extends CollectorConfigLoader {

  import MetricSystem.ConfigRoot

  private val DefaultConfigPath = "collectors-defaults.rate"

  /**
    * Create a Rate with the following address.  See the documentation for [[colossus.metrics.MetricSystem]] for details on configuration
    *
    * @param address The MetricAddress of this Rate.  Note, this will be relative to the containing MetricSystem's metricAddress.
    * @param ns The namespace to which this Metric is relative.
    * @return
    */
  def apply(address : MetricAddress)(implicit ns : MetricNamespace) : Rate = {
    apply(address, DefaultConfigPath)
  }
  /**
    * Create a Rate with the following address, whose definitions is contained the specified configPath.
    *
    * @param address    The MetricAddress of this Rate.  Note, this will be relative to the containing MetricSystem's metricAddress.
    * @param configPath The path in the config that this rate's configuration is located.  This is relative to the MetricSystem config
    *                   definition.
    * @param ns The namespace to which this Metric is relative.
    * @return
    */
  def apply(address : MetricAddress, configPath : String)(implicit ns : MetricNamespace) : Rate = {
    addToNamespace(address, configPath, None)
  }

  /**
    * Create a Rate with the following address.  Source the config from the provided Config object,
    * instead of the MetricNamespace's Config
    *
    * @param address The MetricAddress of this Rate.  Note, this will be relative to the containing MetricSystem's metricAddress.
    * @param configPath The path to this Rate's configuration within the `externalConfig`.
    * @param externalConfig  A Config object which is expected to contain all the necessary fields for creating a Rate
    * @param ns
    * @return
    */
  def apply(address : MetricAddress, configPath : String, externalConfig : Config)(implicit ns : MetricNamespace) : Rate = {
    addToNamespace(address, configPath, Some(externalConfig))
  }

  private def addToNamespace(address : MetricAddress, configPath : String, externalConfig : Option[Config])(implicit ns : MetricNamespace) : Rate = {
    ns.getOrAdd(address){(fullAddress, config) =>
      val addressPath = fullAddress.pieceString.replace('/','.')
      val c = externalConfig.getOrElse(config.config)
      val params = resolveConfig(c, addressPath, s"$ConfigRoot.$configPath", s"$ConfigRoot.$DefaultConfigPath")
      createRate(fullAddress, params.getBoolean("prune-empty"), params.getBoolean("enabled"), config.intervals)
    }
  }

  /**
    * Create a new Rate which will be contained by the specified Collection
    *
    * @param address The MetricAddress of this Rate.  Note, this will be relative to the containing MetricSystem's metricAddress.
    * @param pruneEmpty Instruct the collector to not report any values for tag combinations which were previously empty.
    * @param enabled If this Rate will actually be collected and reported.
    * @param ns The namespace to which this Metric is relative.
    * @return
    */
  def apply(address: MetricAddress, pruneEmpty: Boolean = false, enabled : Boolean = true)(implicit ns: MetricNamespace): Rate = {
    ns.getOrAdd(address){ (fullAddress, config) =>
      createRate(fullAddress, pruneEmpty, enabled, config.intervals)
    }
  }

  private def createRate(address: MetricAddress, pruneEmpty: Boolean, enabled : Boolean, intervals : Seq[FiniteDuration]) : Rate = {
    if(enabled){
      new DefaultRate(address, pruneEmpty, intervals)
    }else{
      new NopRate(address, pruneEmpty)
    }
  }
}

