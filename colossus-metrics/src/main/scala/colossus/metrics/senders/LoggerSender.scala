package colossus.metrics

import akka.actor._
import colossus.metrics.senders.MetricsLogger
import colossus.metrics.senders.MetricsLogger.Formatter

/**
 * Simple sender that just prints the stats to the log
 */
class LoggerSenderActor(override val formatter:Formatter) extends Actor with ActorLogging with  MetricsLogger {

  def receive = {
    case s: MetricSender.Send => {
      logMetrics(s)
      log.info(s"Logged ${s.metrics.size} stats")
    }
  }

  override def preStart() {
    log.info("starting dummy stats sender")
  }

  override def postStop() {
    log.info("shutting down dummy sender")
  }
}

class LoggerSender(val formatter:Formatter) extends MetricSender {
  override def name: String = "logger"

  override def props: Props = Props(classOf[LoggerSenderActor], formatter)
}

object LoggerSender extends MetricSender {
  def name = "logger"
  def props = Props(classOf[LoggerSenderActor], MetricsLogger.defaultMetricsFormatter)

  def apply(formatter:Formatter) = new LoggerSender(formatter)
}

