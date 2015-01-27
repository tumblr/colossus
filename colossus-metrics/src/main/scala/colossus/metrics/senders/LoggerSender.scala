package colossus.metrics

import akka.actor._
import colossus.metrics.senders.MetricsLogger

/**
 * Simple sender that just prints the stats to the log
 */
class LoggerSenderActor extends Actor with ActorLogging with  MetricsLogger {

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

object LoggerSender extends MetricSender {
  def name = "logger"
  def props = Props(classOf[LoggerSenderActor])
}

