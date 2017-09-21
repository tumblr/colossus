package colossus.metrics

import colossus.metrics.logging.ColossusLogging


package object senders {

  trait MetricsLogger { this: ColossusLogging =>
    import MetricsLogger.Formatter
    val formatter: Formatter = MetricsLogger.defaultMetricsFormatter

    def logMetrics(s: MetricSender.Send): Unit = {
      val frags = s.fragments
      frags.foreach(stat => debug(formatter(stat, s.timestamp)))
    }
  }

  object MetricsLogger {
    type Formatter = (MetricFragment, Long) => String
    val defaultMetricsFormatter = (frag: MetricFragment, timestamp: Long) =>
      OpenTsdbFormatter.format(frag, timestamp).stripLineEnd
  }
}
