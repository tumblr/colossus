package colossus.metrics

import akka.util.Timeout
import colossus.metrics.MetricDatabase.ListCollectors
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


    //note - this test passes locally, but seems go never work in travis
    //need to make some design changes any to eliminate the non-determinism
    "two metric systems don't react to each other's ticks" taggedAs(org.scalatest.Tag("test")) ignore {
      implicit val sys = ActorSystem("metrics")
      //set the tick period to something really high so we can control the ticks ourselves
      val m1 = MetricSystem("/sys1", tickPeriod = 10.days, collectSystemMetrics = false)
      val m2 = MetricSystem("/sys2", tickPeriod = 10.days, collectSystemMetrics = false)

      val m1col = m1.sharedCollection()
      val m1counter = m1col.getOrAdd(Counter("/foo"))
      m1counter.increment()
      m1counter.increment()
      m1counter.increment()

      val m2col = m2.sharedCollection()
      val m2counter = m2col.getOrAdd(Counter("/bar"))
      m2counter.increment()
      m2counter.increment()

      //first tick triggers collectors to send metrics, second tick tells
      //MetricDatabase to publish the snapshot from the previous tick
      sys.eventStream.publish(MetricClock.Tick(m1.id, 1))
      Thread.sleep(500)
      sys.eventStream.publish(MetricClock.Tick(m1.id, 2))
      Thread.sleep(50)

      def wait(num: Int)(f: => Boolean) {
        def loop(n: Int) {
          if (!f) {
            if (n == 0) {
              fail(s"Check failed after $num tries")
            } else {
              Thread.sleep(50)
              loop(n - 1)
            }
          }
        }
        loop(num)
      }
        
      
      wait(50){     
        m1.snapshot() == (Map(Root / "foo" -> Map(TagMap.Empty -> 3)))
      }
      m2.snapshot() must equal(Map())

      sys.eventStream.publish(MetricClock.Tick(m2.id, 1))
      Thread.sleep(500)
      sys.eventStream.publish(MetricClock.Tick(m2.id, 2))
      Thread.sleep(50)

      wait(50){
        m2.snapshot() == Map(Root / "bar" -> Map(TagMap.Empty -> 2))
      }
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
      val c: Future[Set[ActorRef]] = (m1.database ? ListCollectors).mapTo[Set[ActorRef]]
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

      val c: Future[Set[ActorRef]] = (m1.database ? ListCollectors).mapTo[Set[ActorRef]]
      c.futureValue must equal (Set(sc1.collector))
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
