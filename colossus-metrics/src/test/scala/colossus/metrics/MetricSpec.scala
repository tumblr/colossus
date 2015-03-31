package colossus.metrics

import akka.util.Timeout
import colossus.metrics.IntervalAggregator._
import org.scalatest._

import akka.actor._
import scala.concurrent.duration._
import MetricAddress._


class MetricSpec(_system : ActorSystem) extends MetricIntegrationSpec(_system) with OptionValues with MetricSystemMatchers {

  def this() = this(ActorSystem("MetricSpec"))

  implicit val sys = _system

  import akka.testkit._

  implicit val ec = sys.dispatcher

  override protected def afterAll() {
    TestKit.shutdownActorSystem(sys)
  }

  "MetricAddress" must {
    "startsWith" in {
      val big = Root / "foo" / "bar" / "baz"
      val small = Root / "foo" / "bar"
      val nope = Root / "bar" / "baz"
      big.startsWith(small) must equal (true)
      big.startsWith(nope) must equal (false)
    }
  }

  "MetricSystem" must {
    "allow multiple systems to start without any conflicts" in {
      val m1 = MetricSystem("/sys1")
      val m2 = MetricSystem("/sys2")
      //no exceptions means the test passed
    }

    "register EventCollectors across aggregators" in {
      //set the tick period to something really high so we can control the ticks ourselves
      implicit val m1 = MetricSystem("/sys3", Seq(1.day, 10.days), false)

      val p1 = SharedCollection()
      val p2 = SharedCollection()

      val expColl = Set(p1.collector, p2.collector)

      m1 must haveCollectors(expColl)

    }

    "remove terminated EventCollectors" in {
      //set the tick period to something really high so we can control the ticks ourselves
      implicit val m1 = MetricSystem("/sys1", Seq(1.day, 10.days), false)

      val sc1 = SharedCollection()
      val sc2 = SharedCollection()

      val expColl = Set(sc1.collector, sc2.collector)

      m1 must haveCollectors(expColl)

      val p = TestProbe()
      p.watch(sc2.collector)

      sc2.collector ! PoisonPill

      p.expectTerminated(sc2.collector)

      m1 must haveCollectors(Set(sc1.collector))
    }

    "Registered Collectors should receive Ticks from all IntervalAggregators" in {
      //set the tick period to something really high so we can control the ticks ourselves
      implicit val m1 = MetricSystem("/sys1", Seq(1.day, 10.days), false)

      val p = TestProbe()
      m1.registerCollector(p.ref)

      m1 must haveCollectors(Set(p.ref))

      m1.metricIntervals.values.foreach(_.intervalAggregator ! SendTick)
      p.expectMsgAllOf(Tick(1, 1.day), Tick(1, 10.days))  //the probe should get ticks from both Aggregators
    }

    "register MetricReporters with an IntervalAggregator" in {
      //set the tick period to something really high so we can control the ticks ourselves
      val intervals = Seq(1.day, 10.days)
      implicit val m1 = MetricSystem("/sys1", intervals, false)

      val oneDayAgg = m1.metricIntervals.get(1.day).value  //grab an interval
      val tenDayAgg = m1.metricIntervals.get(10.days).value  //grab an interval

      val conf = MetricReporterConfig(Root, Seq(LoggerSender), None, None, false)
      val reporter = oneDayAgg.report(conf)

      //only one aggregator should have the reporter
      val expectedReporters = Map(oneDayAgg.intervalAggregator->Set(reporter), tenDayAgg.intervalAggregator->Set[ActorRef]())

      m1 must haveReporters(expectedReporters)
    }

    "remove terminated MetricReporters" in {

      //set the tick period to something really high so we can control the ticks ourselves
      implicit val m1 = MetricSystem("/sys1", Seq(1.day, 10.days), false)

      val oneDayAgg = m1.metricIntervals.get(1.day).value  //grab an interval
      val tenDayAgg = m1.metricIntervals.get(10.days).value  //grab an interval

      val conf = MetricReporterConfig(Root, Seq(LoggerSender), None, None, false)
      val reporter = oneDayAgg.report(conf)

      val expectedReporters = Map(oneDayAgg.intervalAggregator->Set(reporter), tenDayAgg.intervalAggregator->Set[ActorRef]())

      m1 must haveReporters(expectedReporters)

      val p = TestProbe()
      p.watch(reporter)

      reporter ! PoisonPill
      p.expectTerminated(reporter)

      val expectedAfterDeath = Map(oneDayAgg.intervalAggregator->Set[ActorRef](), tenDayAgg.intervalAggregator->Set[ActorRef]())
      m1 must haveReporters(expectedAfterDeath)
    }

    "Registered Reporters should receive ReportMetrics messags only from their IntervalAggregators" in {
      //set the tick period to something really high so we can control the ticks ourselves
      implicit val m1 = MetricSystem("/sys1", Seq(1.day, 10.days), false)

      val p = TestProbe()
      val oneDayAgg = m1.metricIntervals.get(1.day).value  //grab an interval
      val tenDayAgg = m1.metricIntervals.get(10.days).value  //grab an interval

      oneDayAgg.intervalAggregator ! RegisterReporter(p.ref)

      val expectedReporters = Map(oneDayAgg.intervalAggregator->Set(p.ref), tenDayAgg.intervalAggregator->Set[ActorRef]())

      m1 must haveReporters(expectedReporters)

      m1.metricIntervals.values.foreach(_.intervalAggregator ! SendTick)

      p.expectMsg(ReportMetrics(Map(Root /"sys1"/"1day"/"metric_completion" -> Map(Map[String, String]() -> MetricValues.SumValue(0L)))))

    }
  }

  case class Foo(address: MetricAddress) extends MetricProducer {
    def metrics(context: CollectionContext) = Map()
  }

  "JSON Serialization" must {
    import net.liftweb.json._

    "serialize" in {
      val map: RawMetricMap = Map(
        Root / "foo" -> Map(
          Map("a" -> "va") -> 3L,
          Map("b" -> "vb") -> 4L
        )
      )
      val expected = parse(
        """{"/foo" : [ 
          {"tags" : {"a" : "va"}, "value" : 3},
          {"tags" : {"b" : "vb"}, "value" : 4}
          ]}"""
      )

      map.toJson must equal(expected)

    }

    "unserialize" in {
      val expected: RawMetricMap = Map(
        Root / "foo" -> Map(
          Map("a" -> "va") -> 3L,
          Map("b" -> "vb") -> 4L
        )
      )
      val json = parse(
        """{"/foo" : [ 
          {"tags" : {"a" : "va"}, "value" : 3},
          {"tags" : {"b" : "vb"}, "value" : 4}
          ]}"""
      )

      RawMetricMap.fromJson(json) must equal(expected)
    }
      
  }
}
