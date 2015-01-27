package colossus.metrics

import akka.actor._
import colossus.metrics.senders.MetricsLogger
import scala.concurrent.duration._

import java.net._

//TODO : OH jeez don't use raw socket
class OpenTsdbSenderActor(val host: String, val port: Int) extends Actor with ActorLogging with MetricsLogger{

  val address = new InetSocketAddress(host, port)
  val timeout = 500

  case object Initialize

  import context.dispatcher

  val socket = new Socket

  def put(stats: Seq[MetricFragment], ts: Long) {
    val os = socket.getOutputStream
    val now = ts / 1000
    stats.foreach{stat => os.write(OpenTsdbFormatter.format(stat, now).toCharArray.map{_.toByte})}
    os.flush()
    log.info(s"Sent ${stats.size} stats to OpenTSDB")
  }

  def receive = {
    case Initialize => {
      log.info("Initializing new stats sender")
      socket.connect(address, timeout)
      context.become(accepting)
    }
  }

  def accepting: Receive = {
    case s: MetricSender.Send => {
      logMetrics(s)
      put(s.fragments, s.timestamp)
    }
  }

  override def postRestart(reason: Throwable) {
    context.system.scheduler.scheduleOnce(5 seconds, self , Initialize)
  }

  override def preStart() {
    self ! Initialize
  }

}

case class OpenTsdbSender(host: String, port: Int) extends MetricSender {
  val name = "tsdb"
  def props = Props(classOf[OpenTsdbSenderActor], host, port).withDispatcher("opentsdb-dispatcher")
}

