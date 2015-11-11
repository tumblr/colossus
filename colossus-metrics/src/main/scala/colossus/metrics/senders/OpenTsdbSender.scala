package colossus.metrics

import akka.actor._
import colossus.metrics.senders.MetricsLogger
import scala.concurrent.duration._

import scala.concurrent.ExecutionContext.Implicits.global

import java.net._

class OpenTsdbWatchdog(socket: Socket, timeout: FiniteDuration) extends Actor with ActorLogging {

  import OpenTsdbWatchdog._

  private var lastPing = 0

  def receive = idle

  def idle: Receive = {
    case StartSend => {
      val now = System.currentTimeMillis
      context.system.scheduler.scheduleOnce(timeout, self, CheckTimeout(now))
      context.become(timing(now))
    }
  }

  def timing(start: Long): Receive = {
    case EndSend => context.become(idle)
    case CheckTimeout(time) if (time == start) => {
      log.warning("TSDB sender has timed out, force closing socket")
      socket.close()
      self ! PoisonPill
    }
  }
}

object OpenTsdbWatchdog {
  case object StartSend
  case object EndSend
  case class CheckTimeout(time: Long)
}

//TODO : OH jeez don't use raw socket
class OpenTsdbSenderActor(val host: String, val port: Int, timeout: FiniteDuration) extends Actor with ActorLogging with MetricsLogger{
  import OpenTsdbWatchdog._

  val address = new InetSocketAddress(host, port)

  case object Initialize

  val socket = new Socket

  val watchdog = context.actorOf(Props(classOf[OpenTsdbWatchdog], socket, timeout))

  override def postStop(): Unit = {
    socket.close()
    watchdog ! PoisonPill
  }

  def put(stats: Seq[MetricFragment], ts: Long) {
    watchdog ! StartSend
    val os = socket.getOutputStream
    val now = ts / 1000
    stats.foreach{stat => os.write(OpenTsdbFormatter.format(stat, now).toCharArray.map{_.toByte})}
    os.flush()
    log.info(s"Sent ${stats.size} stats to OpenTSDB")
    watchdog ! EndSend
  }

  def receive = {
    case Initialize => {
      log.info("Initializing new stats sender")
      socket.connect(address, 500)
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
  val defaultTimeout: FiniteDuration = 1.minute
  val name = "tsdb"
  def props = Props(classOf[OpenTsdbSenderActor], host, port, defaultTimeout).withDispatcher("opentsdb-dispatcher")
}

