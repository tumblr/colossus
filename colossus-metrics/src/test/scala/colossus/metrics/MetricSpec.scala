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


    "register EventCollectors across aggregators" in {
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

    "remove terminated EventCollectors" in {
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

    "Registered Collectors should receive Ticks from all IntervalAggregators" in {
      implicit val sys = ActorSystem("metrics")
      //set the tick period to something really high so we can control the ticks ourselves
      implicit val m1 = MetricSystem("/sys1", Seq(1.day, 10.days), false)

      val p = TestProbe()
      m1.registerCollector(p.ref)

      m1.metricIntervals.values.foreach(_.intervalAggregator ! SendTick)
      p.expectMsgAllOf(Tick(1, 1.day), Tick(1, 10.days))
      sys.shutdown()
    }

    "register MetricReporters with an IntervalAggregator" in {
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

    "remove terminated MetricReporters" in {
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

    "Registered Reporters should receive ReportMetrics messags only from their IntervalAggregators" in {
      implicit val sys = ActorSystem("metrics")
      //set the tick period to something really high so we can control the ticks ourselves
      implicit val m1 = MetricSystem("/sys1", Seq(1.day, 10.days), false)

      val p = TestProbe()
      val aggregator = m1.metricIntervals.get(1.day).value.intervalAggregator
      aggregator ! RegisterReporter(p.ref)
      m1.metricIntervals.values.foreach(_.intervalAggregator ! SendTick)
      p.expectMsg(ReportMetrics(Map(Root /"sys1"/"1day"/"metric_completion" -> Map(Map[String, String]() -> 0L))))
      sys.shutdown()
    }
  }


  "Metric" must {
    "add a new tagged value" in {
      val v1 = Map("foo" -> "bar") -> 4L
      val v2 = Map("foo" -> "zzz") -> 45L
      val m = Metric(Root, Map(v1))
      val expected = Metric(Root, Map(v1,v2))

      m + v2 must equal (expected)
    }

    "merge with metric of same address" in {
      val m1v1 = Map("a" -> "a") -> 3L
      val m1v2 = Map("b" -> "b") -> 3L
      val m2v1 = Map("a" -> "aa") -> 5L
      val m2v2 = Map("b" -> "bb") -> 6L

      val m1 = Metric(Root / "foo", Map(m1v1,m1v2))
      val m2 = Metric(Root / "foo", Map(m2v1,m2v2))

      val expected = Metric(Root / "foo", Map(m1v1,m1v2,m2v1,m2v2))
      m1 ++ m2 must equal (expected)
    }

      

  }

  case class Foo(address: MetricAddress) extends MetricProducer {
    def metrics(context: CollectionContext) = Map()
  }


  "MetricMapBuilder" must {
    "build a map" in {
      val b = new MetricMapBuilder
      val map1 = Map(
        Root / "foo" -> Map(
          Map("a" -> "va") -> 3L,
          Map("b" -> "vb") -> 4L
        )
      )
      val map2 = Map(
        Root / "foo" -> Map(
          Map("c" -> "vc") -> 3L
        ),
        Root / "bar" -> Map(
          Map("a" -> "ba") -> 45L
        )
      )
      val expected = Map(
        Root / "foo" -> Map(
          Map("a" -> "va") -> 3L,
          Map("b" -> "vb") -> 4L,
          Map("c" -> "vc") -> 3L
        ),
        Root / "bar" -> Map(
          Map("a" -> "ba") -> 45L
        )
      )

      b.add(map1)
      b.add(map2)
      b.result must equal(expected)

      val reversed = new MetricMapBuilder
      reversed.add(map2)
      reversed.add(map1)
      reversed.result must equal(expected)
    }
  }

  "JSON Serialization" must {
    import net.liftweb.json._

    "serialize" in {
      val map = Map(
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
      val expected = Map(
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

      MetricMap.fromJson(json) must equal(expected)
    }
      
  }
}
