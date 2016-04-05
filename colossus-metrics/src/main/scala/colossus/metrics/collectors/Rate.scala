package colossus.metrics

import scala.concurrent.duration._

/**
  * Metrics Collector which increments a value and resets after collection.
  */
trait Rate extends Collector{

  /**
    * Increment value to this Rate for this specified TagMap
    * A single Rate instance divides values amongst TagMaps, and tracks each one independently
    * When they are collected and reported, all TagMaps will be reported under the same MetricAddress.
    * @param tags Tags to record with this value
    * @param amount The value to increment the rate
    */
  def hit(tags : TagMap = TagMap.Empty, amount : Long = 1)

  /**
    * Instruct the collector to not report any values for tag combinations which were previously empty.
    * @return
    */
  def pruneEmpty : Boolean

}

//working implementation of a Rate
class DefaultRate private[metrics](val address: MetricAddress, val pruneEmpty: Boolean)(implicit ns: MetricNamespace) extends Rate {

  private val maps: Map[FiniteDuration, CollectionMap[TagMap]] = ns.collection.config.intervals.map{ i => (i, new CollectionMap[TagMap])}.toMap

  //note - the totals are shared amongst all intervals, and we use the smallest
  //interval to update them
  private val totals = new CollectionMap[TagMap]
  private val minInterval = if (ns.collection.config.intervals.size > 0) ns.collection.config.intervals.min else Duration.Inf

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
class NopRate private[metrics](val address : MetricAddress, val pruneEmpty : Boolean)(implicit ns : MetricNamespace) extends Rate {

  private val empty : MetricMap = Map()

  override def tick(interval: FiniteDuration): MetricMap = empty

  override def hit(tags: TagMap, value: MetricValue): Unit = {}
}

object Rate extends CollectorConfigLoader {

  private val DefaultConfigPath = "colossus.metrics.collectors-defaults.rate"

  /**
    * Create a Rate with the following address.  See the documentation for [[colossus.metrics.MetricSystem for details on configuration]]
    * @param address The MetricAddress of this Rate.  Note, this will be relative to the containing MetricSystem's metricAddress.
    * @param collection The collection which will contain this Collector.
    * @return
    */
  def apply(address : MetricAddress)(implicit ns : MetricNamespace) : Rate = {
    apply(address, MetricSystem.ConfigRoot)
  }
  /**
    * Create a Rate with the following address.
    * This constructor tells the MetricSystem to look for this Metric's configuration outside of the configuration which
    * was used to construct the MetricSystem.
    * @param address The MetricAddress of this Rate.  Note, this will be relative to the containing MetricSystem's metricAddress.
    * @param configPath The path in the ConfigFile that this rate's configuration is located.  This will take precedent over any existing configuration
    *                   inside the MetricSystem.
    * @param collection The collection which will contain this Collector.
    * @return
    */
  def apply(address : MetricAddress, configPath : String)(implicit ns : MetricNamespace) : Rate = {

    val params = resolveConfig(ns.collection.config.config, s"$configPath.$address", DefaultConfigPath)
    apply(address, params.getBoolean("prune-empty"), params.getBoolean("enabled"))
  }

  /**
    * Create a new Rate which will be contained by the specified Collection
    * @param address The MetricAddress of this Rate.  Note, this will be relative to the containing MetricSystem's metricAddress.
    * @param pruneEmpty Instruct the collector to not report any values for tag combinations which were previously empty.
    * @param enabled If this Rate will actually be collected and reported.
    * @param collection The collection which will contain this Collector.
    * @return
    */
  def apply(address: MetricAddress, pruneEmpty: Boolean = false, enabled : Boolean = true)(implicit ns: MetricNamespace): Rate = {
    if(enabled){
      ns.collection.getOrAdd(new DefaultRate(ns.namespace / address, pruneEmpty))
    }else{
      new NopRate(ns.namespace / address, pruneEmpty)
    }
  }
}

