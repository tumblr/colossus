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

  /**
   * Returns the current number of hits to the rate since the last aggregation
   *
   * @param collectionInterval The collection interval to retrieve the value for.  Collection intervals are configured per [[MetricSystem]]
   * @param tags The tags for the value to get, defaults to an empty tagmap
   *
   */
  def value(collectionInterval: FiniteDuration, tags: TagMap = TagMap.Empty): Long

  /**
   * Returns the total number of ticks across all collection intervals to the rate for a set of tags
   *
   * @param tags The tags for the count to get
   */
  def count(tags: TagMap = TagMap.Empty): Long



}

//working implementation of a Rate
private[metrics] class DefaultRate private[metrics](val address: MetricAddress, val pruneEmpty: Boolean, intervals : Seq[FiniteDuration])extends Rate {

  private val maps: Map[FiniteDuration, CollectionMap[TagMap]] = intervals.map{ i => (i, new CollectionMap[TagMap])}.toMap

  //note - the totals are shared amongst all intervals, and we use the smallest
  //interval to update them
  private val totals = new CollectionMap[TagMap]
  private val minInterval = if (intervals.size > 0) intervals.min else Duration.Inf

  def hit(tags: TagMap = TagMap.Empty, amount: Long = 1) {
    maps.foreach{ case (_, map) => map.increment(tags, amount) }
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

  def value(collectionInterval: FiniteDuration, tags: TagMap = TagMap.Empty): Long = {
    maps(collectionInterval).get(tags).getOrElse(0)
  }

  def count(tags: TagMap = TagMap.Empty): Long = {
    minInterval match {
      case f: FiniteDuration => totals.get(tags).getOrElse(0L) + maps(f).get(tags).getOrElse(0L)
      case _ => 0L
    }
  }

}

//Dummy implementation of a Rate, used when "enabled=false" is specified at creation
private[metrics] class NopRate private[metrics](val address : MetricAddress, val pruneEmpty : Boolean)extends Rate {

  private val empty : MetricMap = Map()

  def tick(interval: FiniteDuration): MetricMap = empty

  def hit(tags: TagMap, value: MetricValue): Unit = {}

  def value(collectionInterval: FiniteDuration, tags: TagMap = TagMap.Empty): Long = 0L

  def count(tags: TagMap = TagMap.Empty): Long = 0
}

object Rate {

  private val DefaultConfigPath = "rate"

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
    * See the documentation for [[colossus.metrics.MetricSystem]] for details on configuration
    *
    * @param address    The MetricAddress of this Rate.  Note, this will be relative to the containing MetricSystem's metricAddress.
    * @param configName The path in the config that this rate's configuration is located.  This is relative to the MetricSystem config
    *                   definition.
    * @param ns The namespace to which this Metric is relative.
    * @return
    */
  def apply(address : MetricAddress, configName : String)(implicit ns : MetricNamespace) : Rate = {
    ns.getOrAdd(address){(fullAddress, config) =>
      val params = config.resolveConfig(fullAddress, DefaultConfigPath, configName)
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

