package colossus.metrics

import scala.concurrent.duration._

class Rate private[metrics](val address: MetricAddress, val pruneEmpty: Boolean)(implicit collection: Collection) extends Collector {

  private val maps = collection.config.intervals.map{i => (i, new CollectionMap[TagMap])}.toMap

  //note - the totals are shared amongst all intervals, and we use the smallest
  //interval to update them
  private val totals = new CollectionMap[TagMap]
  private val minInterval = if (collection.config.intervals.size > 0) collection.config.intervals.min else Duration.Inf

  def hit(tags: TagMap = TagMap.Empty, num: Long = 1) {
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

object Rate extends CollectorConfigLoader {

  private val DefaultConfigPath = "colossus.metrics.collectors-defaults.rate"

  /**
    * Create a Rate with the following address.  See the documentation for [[colossus.metrics.MetricSystem for details on configuration]]
    * @param address The MetricAddress of this Rate.  Note, this will be relative to the containing MetricSystem's metricAddress.
    * @param collection The collection which will contain this Collector.
    * @return
    */
  def apply(address : MetricAddress)(implicit collection : Collection) : Rate = {
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
  def apply(address : MetricAddress, configPath : String)(implicit collection : Collection) : Rate = {

    val params = resolveConfig(collection.config.config, s"$configPath.$address", DefaultConfigPath)
    apply(address, params.getBoolean("prune-empty"))
  }

  /**
    * Create a new Rate which will be contained by the specified Collection
    * @param address The MetricAddress of this Rate.  Note, this will be relative to the containing MetricSystem's metricAddress.
    * @param pruneEmpty Instruct the collector to not report any values for tag combinations which were previously empty.
    * @param collection The collection which will contain this Collector.
    * @return
    */
  def apply(address: MetricAddress, pruneEmpty: Boolean = false)(implicit collection: Collection): Rate = {
    collection.getOrAdd(new Rate(address, pruneEmpty))
  }
}

