package colossus.metrics


case class MetricReport(globalTags: TagMap, metrics: RawMetricMap.SerializedMetricMap, reportingFreqMillis: Int, timestamp: Long)
