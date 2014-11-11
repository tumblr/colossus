package colossus.metrics

import akka.actor._

/**
 * Simple sender that just prints the stats to the log
 */
class LoggerSenderActor(verbose: Boolean = false) extends Actor with ActorLogging {

  def receive = {
    case s: MetricSender.Send => {
      val frags = s.fragments
      if (verbose) {
        frags.foreach{stat => log.info(OpenTsdbFormatter.format(stat, s.timestamp).stripLineEnd)}
      }
      log.info(s"Logged ${frags.size} stats")
    }
  }

  override def preStart() {
    log.info("starting dummy stats sender")
  }

  override def postStop() {
    log.info("shutting down dummy sender")
  }

}

case class LoggerSender(verbose: Boolean = false) extends MetricSender {
  def name = "logger"
  def props = Props(classOf[LoggerSenderActor], verbose)
}

