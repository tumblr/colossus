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
      val config = MetricSystemConfig.load("foo")

      val m1 = MetricSystem(config)
      val m2 = MetricSystem(config.copy(name = "different-name"))
      //no exceptions means the test passed
    }
  }

  "SystemMetricsCollector" must {
    "generate system metrics" in {
      implicit val ns = TestNamespace() / "foo" * ("a" -> "b")
      val s = new SystemMetricsCollector(ns)
      val m = s.metrics

      m contains "/foo/system/gc/msec" mustBe true
      m contains "/foo/system/gc/cycles" mustBe true
      m contains "/foo/system/fd_count" mustBe true
      m contains "/foo/system/memory" mustBe true

      //verify the namespace's tags are being added
      m("/foo/system/fd_count") contains (Map("a" -> "b")) mustBe true
    }

  }
}
