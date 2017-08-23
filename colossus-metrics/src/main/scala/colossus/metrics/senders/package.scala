package colossus.metrics

import akka.actor.ActorLogging

package object senders {

  trait MetricsLogger { this: ActorLogging =>
    import MetricsLogger.Formatter
    val formatter: Formatter = MetricsLogger.defaultMetricsFormatter

    def logMetrics(s: MetricSender.Send): Unit = {
      val frags = s.fragments
      if (log.isDebugEnabled)
        frags.foreach(stat => log.debug(formatter(stat, s.timestamp)))
    }
  }

  object MetricsLogger {
    type Formatter = (MetricFragment, Long) => String
    val defaultMetricsFormatter = (frag: MetricFragment, timestamp: Long) =>
      OpenTsdbFormatter.format(frag, timestamp).stripLineEnd
  }
}
