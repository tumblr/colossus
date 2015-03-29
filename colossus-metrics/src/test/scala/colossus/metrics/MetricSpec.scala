package colossus.metrics

import akka.testkit.TestProbe
import akka.util.Timeout
import colossus.metrics.IntervalAggregator._
import org.scalatest._


import akka.actor._
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.Future
import scala.concurrent.duration._
import MetricAddress._

class MetricSpec extends WordSpec with MustMatchers with BeforeAndAfterAll with ScalaFutures with OptionValues {

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
      implicit val sys = ActorSystem("metrics")
      val m1 = MetricSystem("/sys1")
      val m2 = MetricSystem("/sys2")
      //no exceptions means the test passed
      sys.shutdown()
    }

    //TODO: FIX ALL THESE TESTS..TIMING ISSUES IN TRAVIS
    "register EventCollectors across aggregators" ignore {
      import akka.pattern.ask

      implicit val sys = ActorSystem("metrics")
      implicit val ec = sys.dispatcher
      //set the tick period to something really high so we can control the ticks ourselves
      implicit val m1 = MetricSystem("/sys1", Seq(1.day, 10.days), false)

      implicit val to = new Timeout(1.second)

      val sc1 = SharedCollection()
      val sc2 = SharedCollection()

      Thread.sleep(50)

      val f: Future[Iterable[Set[ActorRef]]] = Future.sequence(m1.metricIntervals.values.map(x => (x.intervalAggregator ? ListCollectors).mapTo[Set[ActorRef]]))
      val expectedCollectors = Set(sc1.collector,sc2.collector)
      val expectedValue = Iterable.fill(2)(expectedCollectors)
      f.futureValue must equal (expectedValue)
      sys.shutdown()
    }

    "remove terminated EventCollectors" ignore {
      import akka.pattern.ask

      implicit val sys = ActorSystem("metrics")
      implicit val ec = sys.dispatcher
      //set the tick period to something really high so we can control the ticks ourselves
      implicit val m1 = MetricSystem("/sys1", Seq(1.day, 10.days), false)

      implicit val to = new Timeout(1.second)

      val sc1 = SharedCollection()
      val sc2 = SharedCollection()

      Thread.sleep(20)

      sc2.collector ! PoisonPill

      Thread.sleep(20)

      val f: Future[Iterable[Set[ActorRef]]] = Future.sequence(m1.metricIntervals.values.map(x => (x.intervalAggregator ? ListCollectors).mapTo[Set[ActorRef]]))
      f.futureValue.flatten must equal (Iterable.fill(2)(sc1.collector))
      sys.shutdown()
    }

    "Registered Collectors should receive Ticks from all IntervalAggregators" ignore {
      implicit val sys = ActorSystem("metrics")
      //set the tick period to something really high so we can control the ticks ourselves
      implicit val m1 = MetricSystem("/sys1", Seq(1.day, 10.days), false)

      val p = TestProbe()
      m1.registerCollector(p.ref)

      m1.metricIntervals.values.foreach(_.intervalAggregator ! SendTick)
      p.expectMsgAllOf(Tick(1, 1.day), Tick(1, 10.days))
      sys.shutdown()
    }

    "register MetricReporters with an IntervalAggregator" ignore {
      import akka.pattern.ask

      implicit val sys = ActorSystem("metrics")
      implicit val ec = sys.dispatcher

      //set the tick period to something really high so we can control the ticks ourselves
      val intervals = Seq(1.day, 10.days)
      implicit val m1 = MetricSystem("/sys1", intervals, false)

      implicit val to = new Timeout(1.second)

      val interval = m1.metricIntervals.get(1.day).value

      val conf = MetricReporterConfig(LoggerSender, None, None, false)
      val reporter = interval.report(conf)

      Thread.sleep(20)

      val expectedValues = Set(1.day->Set(reporter), 10.days -> Set())
      //sorry..trying to avoid sleeps and awaits and such
      val f: Future[Iterable[(FiniteDuration, Set[ActorRef])]] = Future.sequence(m1.metricIntervals.values.map(x => (x.intervalAggregator ? ListReporters).mapTo[Set[ActorRef]].map((x.interval->_))))
      f.futureValue.toSet must equal(expectedValues)
      sys.shutdown()

    }

    "remove terminated MetricReporters" ignore {
      import akka.pattern.ask

      implicit val sys = ActorSystem("metrics")
      implicit val ec = sys.dispatcher

      //set the tick period to something really high so we can control the ticks ourselves
      implicit val m1 = MetricSystem("/sys1", Seq(1.day, 10.days), false)

      implicit val to = new Timeout(1.second)

      val interval = m1.metricIntervals.get(1.day).value
      val conf = MetricReporterConfig(LoggerSender, None, None, false)
      val reporter = interval.report(conf)

      Thread.sleep(20)

      reporter ! PoisonPill

      Thread.sleep(20)

      val f: Future[Iterable[Set[ActorRef]]] = Future.sequence(m1.metricIntervals.values.map(x => (x.intervalAggregator ? ListReporters).mapTo[Set[ActorRef]]))
      f.futureValue.flatten must equal (Iterable())
      sys.shutdown()
    }

    "Registered Reporters should receive ReportMetrics messags only from their IntervalAggregators" ignore {
      implicit val sys = ActorSystem("metrics")
      //set the tick period to something really high so we can control the ticks ourselves
      implicit val m1 = MetricSystem("/sys1", Seq(1.day, 10.days), false)

      val p = TestProbe()
      val aggregator = m1.metricIntervals.get(1.day).value.intervalAggregator
      aggregator ! RegisterReporter(p.ref)
      m1.metricIntervals.values.foreach(_.intervalAggregator ! SendTick)
      p.expectMsg(ReportMetrics(Map(Root /"sys1"/"1day"/"metric_completion" -> Map(Map[String, String]() -> MetricValues.SumValue(0L)))))
      sys.shutdown()
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
