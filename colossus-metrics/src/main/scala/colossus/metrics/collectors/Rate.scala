package colossus.metrics

import com.typesafe.config.Config

import scala.concurrent.duration._


case class RateParameterDefaults(pruneEmpty : Boolean = false) extends CollectorParameterDefaults

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
    * Create a Rate with the following address.  Note, the address will be prefixed by the MetricSystem's root.
    * Configuration is resolved and overlayed as follows('metricSystemConfigPath' is the config path, if any, that was
    * passed into the MetricSystem.apply function):
    * 1) metricSystemConfigPath.address
    * 2) metricSystemConfigPath.default-collectors.rate
    * 3) colossus.metrics.default-collectors.rate
 *
    * @param address The address relative to the Collection's MetricSystem Root.
    * @param collection The Collection this Metric will become a part of.
    * @return Created Rate.
    */
  def apply(address : MetricAddress)(implicit collection : Collection) : Rate = {

    /*
      wait, you are lying! This isn't looking at the "metricSystemConfigPath"!
      Yes, yes it is! Don't forget that in the MetricSystem.apply, we are merging the supplied path with the reference path and
      writing that as "colossus.metrics".
     */
    val params = resolveConfig(collection.config.config, s"colossus.metrics.$address", DefaultConfigPath)
    apply(address, params.getBoolean("prune-empty"))
  }

  /**
    * Create a Rate with the following address.  Note, the address will be prefixed by the MetricSystem's root.
    * Configuration is resolved and overlayed as follows('metricSystemConfigPath' is the config path, if any, that was
    * passed into the MetricSystem.apply function):
    * 1) configPath.$address
    * 2) metricSystemConfigPath.default-collectors.rate
    * 3) colossus.metrics.default-collectors.rate
 *
    * @param address The address relative to the Collection's MetricSystem Root.
    * @param configPath The path in the ConfigFile that this rate is located.
    * @param collection The Collection this Metric will become a part of.
    * @return
    */
  def apply(address : MetricAddress, configPath : String)(implicit collection : Collection) : Rate = {

    val params = resolveConfig(collection.config.config, s"$configPath.$address", DefaultConfigPath)
    apply(address, params.getBoolean("prune-empty"))
  }

  def apply(address: MetricAddress, pruneEmpty: Boolean = false)(implicit collection: Collection): Rate = {
    collection.getOrAdd(new Rate(address, pruneEmpty))
  }
}

private[metrics] trait CollectorConfigLoader {

  def resolveConfig(config : Config,
                    metricPath : String,
                    defaultCollectorPath : String) : Config = {
    if(config.hasPath(metricPath)) {
      config.getConfig(metricPath).withFallback(config.getConfig(defaultCollectorPath))
    }else{
      config.getConfig(defaultCollectorPath)
    }

  }

}


