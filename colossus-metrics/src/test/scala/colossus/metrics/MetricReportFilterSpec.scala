package colossus.metrics

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.TestProbe
import colossus.metrics.IntervalAggregator.ReportMetrics
import colossus.metrics.MetricValues.SumValue

class MetricReportFilterSpec(_system : ActorSystem) extends MetricIntegrationSpec(_system) with MetricSystemMatchers {

  def this() = this(ActorSystem("MetricSpec"))

  implicit val sys = _system

  val metricMap: MetricMap = Map(MetricAddress("/foo")->Map(Map("foo" -> "a")->SumValue(0L)),
                                  MetricAddress("/bar")->Map(Map("bar" -> "a")->SumValue(1L)))

  "MetricReportFilters" must {

    "Filter nothing if MetricReporterFilter.All is used" in {
      val expectedRawMetrics: RawMetricMap = Map(MetricAddress("/foo")->Map(Map("foo" -> "a")->0L),
        MetricAddress("/bar")->Map(Map("bar" -> "a")->1L))
      testFilter(MetricReporterFilter.All, expectedRawMetrics)

    }

    "Filter everything if MetricReporterFilter.None is used" in {
      testFilter(MetricReporterFilter.None, Map())
    }

    "Only include matching MetricAddresses if MetricReporterFilter.WhiteList is used" in {
      val expectedRawMetrics: RawMetricMap = Map(MetricAddress("/foo")->Map(Map("foo" -> "a")->0L))
      testFilter(MetricReporterFilter.WhiteList(Seq(MetricAddress("/foo"))), expectedRawMetrics)
    }

    "Filter out metrics if MetricReporterFilter.BlackList is used" in {
      val expectedRawMetrics: RawMetricMap = Map(MetricAddress("/bar")->Map(Map("bar" -> "a")->1L))
      testFilter(MetricReporterFilter.BlackList(Seq(MetricAddress("/foo"))), expectedRawMetrics)
    }
  }

  private def testFilter(filter : MetricReporterFilter, expected : RawMetricMap) {
    val probe = TestProbe()

    val mockAggregator = TestProbe()

    val config = MetricReporterConfig("/sys1", Seq(EchoSender(probe.ref)), None, filter, false)

    val reporter = sys.actorOf(Props(classOf[MetricReporter], mockAggregator.ref, config))

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
