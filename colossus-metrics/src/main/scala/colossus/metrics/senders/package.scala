package colossus.metrics

import akka.actor.ActorLogging

package object senders {

  trait MetricsLogger{ this : ActorLogging =>

    def logMetrics(s : MetricSender.Send) {
      val frags = s.fragments
      if (log.isDebugEnabled) {
        frags.foreach{stat => log.debug(OpenTsdbFormatter.format(stat, s.timestamp).stripLineEnd)}
      }

    }
  }

}
