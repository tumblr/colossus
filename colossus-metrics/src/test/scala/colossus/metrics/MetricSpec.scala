package colossus.metrics

import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import colossus.metrics.IntervalAggregator._
import org.scalatest._


import akka.actor._
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import scala.concurrent.Future
import scala.concurrent.duration._
import MetricAddress._

class MetricSpec(_system : ActorSystem) extends MetricIntegrationSpec(_system) with ScalaFutures with OptionValues
  with IntegrationPatience with EventuallyEquals {

  def this() = this(ActorSystem("MetricSpec"))

  implicit val sys = _system

  import akka.testkit._

  implicit val to = new Timeout(1.second.dilated)
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

    //TODO: FIX ALL THESE TESTS..TIMING ISSUES IN TRAVIS
    "register EventCollectors across aggregators" ignore {
      import akka.pattern.ask

      //set the tick period to something really high so we can control the ticks ourselves
      implicit val m1 = MetricSystem("/sys3", Seq(1.day, 10.days), false)

      val p1 = TestProbe()
      val p2 = TestProbe()

      m1.registerCollector(p1.ref)
      m1.registerCollector(p2.ref)
      p1.expectMsg(Registered)
      p2.expectMsg(Registered)

      val f: Future[Iterable[Set[ActorRef]]] = Future.sequence(m1.metricIntervals.values.map(x => (x.intervalAggregator ? ListCollectors).mapTo[Set[ActorRef]]))
      val expectedCollectors = Set(p1.ref, p2.ref)
      val expectedValue = Iterable.fill(2)(expectedCollectors)
      f.futureValue must equal (expectedValue)
    }

    "remove terminated EventCollectors" ignore {
      import akka.pattern.ask

      //set the tick period to something really high so we can control the ticks ourselves
      implicit val m1 = MetricSystem("/sys1", Seq(1.day, 10.days), false)

      val sc1 = SharedCollection()
      val sc2 = SharedCollection()

      val p = TestProbe()
      p.watch(sc2.collector)

      sc2.collector ! PoisonPill

      p.expectTerminated(sc2.collector)

      val f: Future[Iterable[Set[ActorRef]]] = Future.sequence(m1.metricIntervals.values.map(x => (x.intervalAggregator ? ListCollectors).mapTo[Set[ActorRef]]))
      f.futureValue.flatten must equal (Iterable.fill(2)(sc1.collector))
    }

    "Registered Collectors should receive Ticks from all IntervalAggregators" ignore {
      implicit val sys = ActorSystem("metrics")

      //set the tick period to something really high so we can control the ticks ourselves
      implicit val m1 = MetricSystem("/sys1", Seq(1.day, 10.days), false)

      val p = TestProbe()
      m1.registerCollector(p.ref)
      p.expectMsgAllOf(Registered, Registered)
      m1.metricIntervals.values.foreach(_.intervalAggregator ! SendTick)
      p.expectMsgAllOf(Tick(1, 1.day), Tick(1, 10.days))
    }

    "register MetricReporters with an IntervalAggregator" ignore {
      import akka.pattern.ask

      //set the tick period to something really high so we can control the ticks ourselves
      val intervals = Seq(1.day, 10.days)
      implicit val m1 = MetricSystem("/sys1", intervals, false)

      val interval = m1.metricIntervals.get(1.day).value

      val conf = MetricReporterConfig(LoggerSender, None, None, false)
      val reporter = interval.report(conf)

      val expectedValues: Set[(FiniteDuration, Set[ActorRef])] = Set(1.day->Set(reporter), 10.days -> Set())


      def u = {
        Future.sequence(m1.metricIntervals.values.map(x => (x.intervalAggregator ? ListReporters).mapTo[Set[ActorRef]].map((x.interval->_)))).map(_.toSet)
      }
      val f: () => Future[Set[(FiniteDuration, Set[ActorRef])]] = u _

      f must eventuallyEqual (expectedValues)
    }

    "remove terminated MetricReporters" ignore {
      import akka.pattern.ask

      //set the tick period to something really high so we can control the ticks ourselves
      implicit val m1 = MetricSystem("/sys1", Seq(1.day, 10.days), false)

      val interval = m1.metricIntervals.get(1.day).value
      val conf = MetricReporterConfig(LoggerSender, None, None, false)
      val reporter = interval.report(conf)

      val p = TestProbe()
      p.watch(reporter)
      reporter ! PoisonPill
      p.expectTerminated(reporter)

      val f: Future[Iterable[Set[ActorRef]]] = Future.sequence(m1.metricIntervals.values.map(x => (x.intervalAggregator ? ListReporters).mapTo[Set[ActorRef]]))
      f.futureValue.flatten must equal (Iterable())
    }

    "Registered Reporters should receive ReportMetrics messags only from their IntervalAggregators" ignore {
      implicit val sys = ActorSystem("metrics")

      //set the tick period to something really high so we can control the ticks ourselves
      implicit val m1 = MetricSystem("/sys1", Seq(1.day, 10.days), false)

      val p = TestProbe()
      val aggregator = m1.metricIntervals.get(1.day).value.intervalAggregator
      aggregator ! RegisterReporter(p.ref)
      p.expectMsg(Registered)
      m1.metricIntervals.values.foreach(_.intervalAggregator ! SendTick)

      p.expectMsg(ReportMetrics(Map(Root /"sys1"/"1day"/"metric_completion" -> Map(Map[String, String]() -> MetricValues.SumValue(0L)))))

    }

    def dilatedWait(millis : Long){
      Thread.sleep(millis.milliseconds.dilated.toMillis)
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
