package colossus.metrics.senders

import akka.actor._
import colossus.metrics.MetricSender
import colossus.metrics.logging.ColossusLogging
import colossus.metrics.senders.MetricsLogger.Formatter

/**
  * Simple sender that just prints the stats to the log
  */
class LoggerSenderActor(override val formatter: Formatter) extends Actor with ColossusLogging with MetricsLogger {

  def receive = {
    case s: MetricSender.Send => {
      logMetrics(s)
      info(s"Logged ${s.metrics.size} stats")
    }
  }

  override def preStart() {
    info("starting dummy stats sender")
  }

  override def postStop() {
    info("shutting down dummy sender")
  }
}

class LoggerSender(val formatter: Formatter) extends MetricSender {
  def name: String = "logger"

  def props: Props = Props(classOf[LoggerSenderActor], formatter)
}

object LoggerSender extends MetricSender {
  def name  = "logger"
  def props = Props(classOf[LoggerSenderActor], MetricsLogger.defaultMetricsFormatter)

  def apply(formatter: Formatter) = new LoggerSender(formatter)
}
