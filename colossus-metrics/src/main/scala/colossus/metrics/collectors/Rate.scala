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
class DefaultRate private[metrics](val address: MetricAddress, val pruneEmpty: Boolean)(implicit collection: Collection) extends Rate {

  private val maps: Map[FiniteDuration, CollectionMap[TagMap]] = collection.config.intervals.map{ i => (i, new CollectionMap[TagMap])}.toMap

  //note - the totals are shared amongst all intervals, and we use the smallest
  //interval to update them
  private val totals = new CollectionMap[TagMap]
  private val minInterval = if (collection.config.intervals.size > 0) collection.config.intervals.min else Duration.Inf

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
class NopRate private[metrics](val address : MetricAddress, val pruneEmpty : Boolean)(implicit collection : Collection) extends Rate {

  private val empty : MetricMap = Map()

  override def tick(interval: FiniteDuration): MetricMap = empty

  override def hit(tags: TagMap, value: MetricValue): Unit = {}
}

object Rate extends CollectorConfigLoader {

  import MetricSystem.ConfigRoot

  private val DefaultConfigPath = "collectors-defaults.rate"

  /**
    * Create a Rate with the following address.  See the documentation for [[colossus.metrics.MetricSystem]] for details on configuration
    * @param address The MetricAddress of this Rate.  Note, this will be relative to the containing MetricSystem's metricAddress.
    * @param collection The collection which will contain this Collector.
    * @return
    */
  def apply(address : MetricAddress)(implicit collection : Collection) : Rate = {
    apply(address, DefaultConfigPath)
  }
  /**
    * Create a Rate with the following address, whose definitions is contained the specified configPath.
    * @param address    The MetricAddress of this Rate.  Note, this will be relative to the containing MetricSystem's metricAddress.
    * @param configPath The path in the config that this rate's configuration is located.  This is relative to the MetricSystem config
    *                   definition.
    * @param collection The collection which will contain this Collector.
    * @return
    */
  def apply(address : MetricAddress, configPath : String)(implicit collection : Collection) : Rate = {
    collection.getOrAdd{
      val params = resolveConfig(collection.config.config, s"$ConfigRoot.$configPath", s"$ConfigRoot.$DefaultConfigPath")
      createRate(address, params.getBoolean("prune-empty"), params.getBoolean("enabled"))
    }
  }

  /**
    * Create a new Rate which will be contained by the specified Collection
    * @param address The MetricAddress of this Rate.  Note, this will be relative to the containing MetricSystem's metricAddress.
    * @param pruneEmpty Instruct the collector to not report any values for tag combinations which were previously empty.
    * @param enabled If this Rate will actually be collected and reported.
    * @param collection The collection which will contain this Collector.
    * @return
    */
  def apply(address: MetricAddress, pruneEmpty: Boolean = false, enabled : Boolean = true)(implicit collection: Collection): Rate = {
    collection.getOrAdd(createRate(address, pruneEmpty, enabled))
  }

  private def createRate(address: MetricAddress, pruneEmpty: Boolean, enabled : Boolean)(implicit collection : Collection) : Rate = {
    if(enabled){
      new DefaultRate(address, pruneEmpty)
    }else{
      new NopRate(address, pruneEmpty)
    }
  }
}

