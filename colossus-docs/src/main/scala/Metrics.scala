import akka.actor.ActorSystem
import colossus.metrics.collectors.{Counter, Histogram, Rate}
import colossus.metrics.senders.OpenTsdbSender
import colossus.metrics.{MetricReporterConfig, MetricReporterFilter, MetricSystem}

import scala.concurrent.duration._

object Metrics extends App {

  def example1: Unit = {
    implicit val actorSystem  = ActorSystem()
    implicit val metricSystem = MetricSystem("name")

    // #example1
    val rate = Rate("my-rate")
    rate.hit(Map("endpoint" -> "foo"))
    rate.hit(Map("endpoint" -> "bar"))
    rate.hit(Map("endpoint" -> "bar"))
    // #example1

  }

  def example2: Unit = {
    // #example2
    implicit val actorSystem  = ActorSystem()
    implicit val metricSystem = MetricSystem("name")

    val rate = Rate("my-rate")
    rate.hit()
    // #example2
  }

  def example3: Unit = {
    // #example3
    implicit val actorSystem = ActorSystem()
    val metricSystem         = MetricSystem("name")

    implicit val namespace = metricSystem / "foo" / "bar"

    // this rate will have the address "/foo/bar/baz"
    val rate = Rate("baz")
    // #example3
  }

  def example4: Unit = {
    implicit val actorSystem  = ActorSystem()
    implicit val metricSystem = MetricSystem("name")

    // #example4
    val counter = Counter("my-counter")
    counter.set(Map("foo"       -> "bar"), 2)
    counter.increment(Map("foo" -> "bar"))
    // #example4

    // #example5
    val rate = Rate("my-rate")
    rate.hit()
    // #example5

    // #example6
    val hist = Histogram("my-histogram", percentiles = List(0.5, 0.99, 0.999))
    hist.add(12)
    hist.add(1)
    hist.add(98765)
    // #example6
  }

  def example7: Unit = {
    // #example7
    implicit val actorSystem  = ActorSystem()
    implicit val metricSystem = MetricSystem("name")

    val reporterConfig = MetricReporterConfig(
      metricSenders = Seq(OpenTsdbSender("host", 123)),
      filters = MetricReporterFilter.All
    )

    // reporter must be attached to a specific collection interval.
    metricSystem.collectionIntervals.get(1.minute).foreach(_.report(reporterConfig))

    // now once per minute the value of this rate and any other metrics will be reported
    val rate = Rate("myrate")
    // #example7
  }
}
