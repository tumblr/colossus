package colossus.examples

import akka.actor._
import colossus.metrics._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object MetricsExample extends App {
  implicit val system = ActorSystem()
  implicit val metrics = MetricSystem(MetricSystemConfig.load("application"))

  val reporterConfig = MetricReporterConfig(
    metricSenders = Seq(LoggerSender(senders.MetricsLogger.defaultMetricsFormatter)),
    filters = MetricReporterFilter.All
  )

  // create 2 threads, sending 3 rate hits each
  def hitRate : Any = {
    0 to 1 foreach { _ =>
      Future {
        val r = Rate("rate")
        0 to 2 foreach { _ => r.hit() }
      }
    }
  }

  //A reporter must be attached to a specific collection interval.
  metrics.collectionIntervals.get(1.second).foreach(_.report(reporterConfig))

  //now once per second the value of this rate and any other metrics will be reported
   hitRate
   Thread.sleep(2100)
   hitRate
}
