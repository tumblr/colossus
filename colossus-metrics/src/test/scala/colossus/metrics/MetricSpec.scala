package colossus.metrics

import com.typesafe.config.ConfigFactory
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
      val m1 = MetricSystem(MetricAddress("/sys1"), Seq(), false, ConfigFactory.empty())
      val m2 = MetricSystem(MetricAddress("/sys2"), Seq(), false, ConfigFactory.empty())
      //no exceptions means the test passed
    }
  }
}
