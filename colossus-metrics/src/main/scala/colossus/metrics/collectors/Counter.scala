package colossus.metrics

import scala.concurrent.duration._

/**
  * Metrics Collector which track Long values.
  * A single Counter instance divides counter values up by tag maps and track each one independently.
  * When they are collected and reported, all TagMaps will be reported under the same MetricAddress.
  */
trait Counter extends Collector {

  /**
    * Increment by the specified amount
    *
    * @param tags Tags to record with this value
    * @param amount The amount to increment
    */
  def increment(tags: TagMap = TagMap.Empty, amount: Long = 1)

  /**
    * Decrement by the specified amount
    *
    * @param tags Tags to record with this value
    * @param amount The amount to decrement
    */
  def decrement(tags: TagMap = TagMap.Empty, amount: Long = 1) = increment(tags, 0 - amount)

  /**
    * Set the Counter to the specified value
    *
    * @param tags Tags to record with this value
    * @param value Value to be set.
    */
  def set(tags: TagMap = TagMap.Empty, value: Long)

  /**
    * Retrieve the value for the specified TagMap
    *
    * @param tags TagMap identifier for fetching the value
    * @return
    */
  def get(tags: TagMap = TagMap.Empty): Long
}

//Working implementation of a Counter
class DefaultCounter private[metrics](val address: MetricAddress)(implicit collection: Collection) extends Counter {

  private val counters = new CollectionMap[TagMap]

  def increment(tags: TagMap = TagMap.Empty, amount: Long = 1) {
    counters.increment(tags, amount)
  }

  def set(tags: TagMap = TagMap.Empty, value: Long) {
    counters.set(tags, value)
  }

  def get(tags: TagMap = TagMap.Empty): Long = counters.get(tags).getOrElse(0)

  def tick(interval: FiniteDuration): MetricMap  = {
    val values = counters.snapshot(false, false)
    if (values.isEmpty) Map() else Map(address -> values)
  }
}

//Dummy implementation of a counter, used when "enabled=false" is specified at creation
class NopCounter private[metrics](val address : MetricAddress) extends Counter {
  val empty : MetricMap = Map()
  override def tick(interval: FiniteDuration): MetricMap = empty

  override def increment(tags: TagMap, amount: MetricValue): Unit = {}

  override def set(tags: TagMap, value: MetricValue): Unit = {}

  override def get(tags: TagMap): MetricValue = 0
}

object Counter extends CollectorConfigLoader{

  import MetricSystem.ConfigRoot

  private val DefaultConfigPath = "collectors-defaults.counter"

  /**
    * Create a Counter with the following address.   See the documentation for [[colossus.metrics.MetricSystem]] for details on configuration
    *
    * @param address The MetricAddress of this Counter.  Note, this will be relative to the containing MetricSystem's metricAddress.
    * @param collection The collection which will contain this Collector.
    * @return
    */
  def apply(address : MetricAddress)(implicit collection : Collection) : Counter = {
    apply(address, DefaultConfigPath)
  }

  /**
    * Create a Counter with following address, whose definitions is contained the specified configPath.
    *
    * @param address The MetricAddress of this Counter.  Note, this will be relative to the containing MetricSystem's metricAddress.
    * @param configPath The path in the config that this counter's configuration is located.  This is relative to the MetricSystem config
    *                   definition.
    * @param collection The collection which will contain this Collector.
    * @return
    */
  def apply(address : MetricAddress, configPath : String)(implicit collection : Collection) : Counter = {
    collection.getOrAdd(address){
      val params = resolveConfig(collection.config.config, s"$ConfigRoot.$configPath", s"$ConfigRoot.$DefaultConfigPath")
      createCounter(address, params.getBoolean("enabled"))
    }
  }

  /**
    * Create a Counter
    *
    * @param address The MetricAddress of this Counter.  Note, this will be relative to the containing MetricSystem's metricAddress.
    * @param enabled If this Counter will actually be collected and reported.
    * @param collection The collection which will contain this Collector.
    * @return
    */
  def apply(address: MetricAddress, enabled: Boolean = true)(implicit collection: Collection): Counter = {
    collection.getOrAdd(address)(createCounter(address, enabled))
  }

  private def createCounter(address : MetricAddress, enabled : Boolean)(implicit collection : Collection) : Counter = {
    if(enabled){
      new DefaultCounter(address)
    }else{
      new NopCounter(address)
    }
  }
}
