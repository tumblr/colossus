package colossus.metrics

import akka.util.Timeout
import colossus.metrics.IntervalAggregator.ListCollectors
import org.scalatest._


import akka.actor._
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.Future
import scala.concurrent.duration._
import MetricAddress._

class MetricSpec extends WordSpec with MustMatchers with BeforeAndAfterAll with ScalaFutures {

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


    "register EventCollectors" in {
      import akka.pattern.ask
      implicit val sys = ActorSystem("metrics")
      //set the tick period to something really high so we can control the ticks ourselves
      implicit val m1 = MetricSystem("/sys1", tickPeriod = 10.days, collectSystemMetrics = false)

      implicit val to = new Timeout(1.second)

      val sc1 = SharedCollection()
      val sc2 = SharedCollection()


      Thread.sleep(50)
      val c: Future[Set[ActorRef]] = (m1.intervalAggregator ? ListCollectors).mapTo[Set[ActorRef]]
      c.futureValue must equal (Set(sc1.collector, sc2.collector))

    }
    "remove terminated EventCollectors" in {
      import akka.pattern.ask
      implicit val sys = ActorSystem("metrics")
      //set the tick period to something really high so we can control the ticks ourselves
      implicit val m1 = MetricSystem("/sys1", tickPeriod = 10.days, collectSystemMetrics = false)

      implicit val to = new Timeout(1.second)

      val sc1 = SharedCollection()
      val sc2 = SharedCollection()

      sc2.collector ! PoisonPill

      Thread.sleep(50)

      val c: Future[Set[ActorRef]] = (m1.intervalAggregator ? ListCollectors).mapTo[Set[ActorRef]]
      c.futureValue must equal (Set(sc1.collector))
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
