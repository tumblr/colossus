package colossus.metrics

class Counter(val address: MetricAddress, config: CollectorConfig) extends Collector {

  private val counters = new CollectionMap[TagMap]

  def increment(tags: TagMap = TagMap.Empty, num: Long = 1) {
    counters.increment(tags, num)
  }

  def set(tags: TagMap = TagMap.Empty, num: Long) {
    counters.set(tags)
  }

  def tick(interval: FiniteDuration): MetricMap  = {
    Map(address -> counters.snapshot(false, false))
  }
    

}
