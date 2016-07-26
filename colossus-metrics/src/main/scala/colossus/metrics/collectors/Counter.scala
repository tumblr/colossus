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
private[metrics] class DefaultCounter private[metrics](val address: MetricAddress) extends Counter {

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
private[metrics] class NopCounter private[metrics](val address : MetricAddress) extends Counter {
  val empty : MetricMap = Map()
  override def tick(interval: FiniteDuration): MetricMap = empty

  override def increment(tags: TagMap, amount: MetricValue): Unit = {}

  override def set(tags: TagMap, value: MetricValue): Unit = {}

  override def get(tags: TagMap): MetricValue = 0
}

object Counter {

  private val DefaultConfigPath = "counter"

  /**
    * Create a Counter with the following address.   See the documentation for [[colossus.metrics.MetricSystem]] for details on configuration
    *
    * @param address The MetricAddress of this Counter.  Note, this will be relative to the containing MetricSystem's metricAddress.
    * @param ns The namespace to which this Metric is relative.
    * @return
    */
  def apply(address : MetricAddress)(implicit ns : MetricNamespace) : Counter = {
    apply(address, DefaultConfigPath)
  }

  /**
    * Create a Counter with following address, whose definitions is contained the specified configPath.
    * See the documentation for [[colossus.metrics.MetricSystem]]
    *
    * @param address The MetricAddress of this Counter.  Note, this will be relative to the containing MetricSystem's metricAddress.
    * @param configPath The path in the config that this counter's configuration is located.  This is relative to the MetricSystem config
    *                   definition.
    * @param ns The namespace to which this Metric is relative.
    * @return
    */
  def apply(address : MetricAddress, configPath : String)(implicit ns : MetricNamespace) : Counter = {
    ns.getOrAdd(address){ (fullAddress, config) =>
      val params = config.resolveConfig(fullAddress, DefaultConfigPath, configPath)
      createCounter(fullAddress, params.getBoolean("enabled"))
    }
  }

  /**
    * Create a Counter
    *
    * @param address The MetricAddress of this Counter.  Note, this will be relative to the containing MetricSystem's metricAddress.
    * @param enabled If this Counter will actually be collected and reported.
    * @param ns The namespace to which this Metric is relative.
    * @return
    */
  def apply(address: MetricAddress, enabled: Boolean = true)(implicit ns : MetricNamespace): Counter = {
    ns.getOrAdd(address){(fullAddress, config) =>
      createCounter(fullAddress, enabled)
    }
  }

  private def createCounter(address : MetricAddress, enabled : Boolean) : Counter = {
    if(enabled){
      new DefaultCounter(address)
    }else{
      new NopCounter(address)
    }
  }
}
