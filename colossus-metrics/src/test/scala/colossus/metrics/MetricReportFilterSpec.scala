package colossus.metrics

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.TestProbe
import colossus.metrics.IntervalAggregator.ReportMetrics
import MetricAddress.Root

class MetricReportFilterSpec(_system : ActorSystem) extends MetricIntegrationSpec(_system) {

  def this() = this(ActorSystem("MetricSpec"))

  implicit val sys = _system

  val metricMap: MetricMap = Map(
    MetricAddress("/foo/foo/foo")->Map(Map("foo" -> "a")->0L),
    MetricAddress("/foo/foo/bar")->Map(Map("foo" -> "a")->0L),
    MetricAddress("/foo/bar/foo")->Map(Map("foo" -> "a")->0L),
    MetricAddress("/bar")->Map(Map("bar" -> "a")->1L))

  "MetricReportFilters" must {

    "Filter nothing if MetricReporterFilter.All is used" in {
      val expectedRawMetrics: MetricMap = Map(
        MetricAddress("/foo/foo/foo")->Map(Map("foo" -> "a")->0L),
        MetricAddress("/foo/foo/bar")->Map(Map("foo" -> "a")->0L),
        MetricAddress("/foo/bar/foo")->Map(Map("foo" -> "a")->0L),
        MetricAddress("/bar")->Map(Map("bar" -> "a")->1L))
      testFilter(MetricReporterFilter.All, expectedRawMetrics)

    }

    "Only include matching MetricAddresses if MetricReporterFilter.WhiteList is used (wild card) - 1" in {
      val expectedRawMetrics: MetricMap = Map(
        MetricAddress("/foo/foo/foo")->Map(Map("foo" -> "a")->0L),
        MetricAddress("/foo/bar/foo")->Map(Map("foo" -> "a")->0L))
      testFilter(MetricReporterFilter.WhiteList(Seq(MetricAddress("/foo/*/foo"))), expectedRawMetrics)
    }

    "Only include matching MetricAddresses if MetricReporterFilter.WhiteList is used (wild card) - 2" in {
      val expectedRawMetrics: MetricMap = Map(
        MetricAddress("/foo/foo/foo")->Map(Map("foo" -> "a")->0L),
        MetricAddress("/foo/bar/foo")->Map(Map("foo" -> "a")->0L),
        MetricAddress("/foo/foo/bar")->Map(Map("foo" -> "a")->0L))
      testFilter(MetricReporterFilter.WhiteList(Seq(MetricAddress("/foo/*"))), expectedRawMetrics)
    }

    "Only include matching MetricAddresses if MetricReporterFilter.WhiteList is used (no wild card) - 1" in {
      val expectedRawMetrics: MetricMap = Map(
        MetricAddress("/foo/bar/foo")->Map(Map("foo" -> "a")->0L))
      testFilter(MetricReporterFilter.WhiteList(Seq(MetricAddress("/foo/bar/foo"))), expectedRawMetrics)
    }

    "Only include matching MetricAddresses if MetricReporterFilter.WhiteList is used (no wild card) - 2" in {
      val expectedRawMetrics: MetricMap = Map.empty
      testFilter(MetricReporterFilter.WhiteList(Seq(MetricAddress("/foo/bar"))), expectedRawMetrics)
    }

    "Filter out metrics if MetricReporterFilter.BlackList is used" in {
      val expectedRawMetrics: MetricMap = Map(MetricAddress("/bar")->Map(Map("bar" -> "a")->1L))
      testFilter(MetricReporterFilter.BlackList(Seq(MetricAddress("/foo/*"))), expectedRawMetrics)
    }
  }

  private def testFilter(filter : MetricReporterFilter, expected : MetricMap) {
    val probe = TestProbe()

    val mockAggregator = TestProbe()

    val config = MetricReporterConfig(Seq(EchoSender(probe.ref)), None, filter, false)

    val reporter = MetricReporter(config, mockAggregator.ref, "sys1")

    mockAggregator.send(reporter, ReportMetrics(metricMap))

    val receivedRawMetrics = probe.expectMsgPF(){
      case MetricSender.Send(rm, g, ms) => rm
    }

    receivedRawMetrics must be (expected)
  }

}


class EchoSenderActor(ref : ActorRef) extends Actor {
  override def receive: Receive = {
    case a => ref ! a
  }
}

case class EchoSender(ref : ActorRef) extends MetricSender {
  override def name: String = "echo-sender"

  override def props: Props = Props(classOf[EchoSenderActor], ref)
}
