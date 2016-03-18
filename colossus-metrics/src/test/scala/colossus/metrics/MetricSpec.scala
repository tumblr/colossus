package colossus.metrics

import org.scalatest._

import akka.actor._
import MetricAddress._


class MetricSpec(_system : ActorSystem) extends MetricIntegrationSpec(_system) with OptionValues {

  def this() = this(ActorSystem("MetricSpec"))

  implicit val sys = _system

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
      val m1 = MetricSystem(MetricAddress("/sys1"))
      val m2 = MetricSystem(MetricAddress("/sys2"))
      //no exceptions means the test passed
    }


  }

  "JSON Serialization" must {
    import net.liftweb.json._

    "serialize" in {
      val map: MetricMap = Map(
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
      val expected: MetricMap = Map(
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
