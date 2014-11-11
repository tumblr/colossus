package colossus.metrics


case class MetricReport(globalTags: TagMap, metrics: MetricMap.SerializedMetricMap, reportingFreqMillis: Int, timestamp: Long)
