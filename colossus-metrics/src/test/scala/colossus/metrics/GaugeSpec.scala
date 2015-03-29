package colossus.metrics

import org.scalatest._
import scala.concurrent.duration._

import MetricValues._

class GaugeSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {


  "BasicGauge" must {
    "set a value" in {
      val params = GaugeParams("/")
      val g = new BasicGauge(params)
      g.set(Some(5L))
      g.value must equal (Some(5L))

      g.set(None)
      g.value must equal (None)
    }

    "expire value" in {
      val params = GaugeParams("/", expireAfter = 1.second, expireTo = Some(9876))
      val g = new BasicGauge(params)
      g.set(Some(5))
      g.value must equal(Some(5))
      g.tick(500.milliseconds)
      g.value must equal(Some(5))
      g.tick(501.milliseconds)
      g.value must equal(Some(9876))
    }
  }

  "ConcreteGauge" must {
    "set tagged values" in {
      val params = GaugeParams("/")
      val g = new ConcreteGauge(params)
      g.set(4, Map("foo" -> "a"))
      g.set(5, Map("foo" -> "b"))

      val expected = Map(
        MetricAddress.Root -> Map(Map("foo" -> "a") -> SumValue(4L), Map("foo" -> "b") -> SumValue(5L))
      )

      g.metrics(CollectionContext(Map())) must equal(expected)
    }

    "remove unset values" in {
      val params = GaugeParams("/", expireAfter = 1.second)
      val g = new ConcreteGauge(params)
      g.set(4, Map("foo" -> "a"))
      g.set(5, Map("foo" -> "b"))
      g.set(None, Map("foo" -> "a"))

      val expected = Map(
        MetricAddress.Root -> Map(Map("foo" -> "b") -> SumValue(5L))
      )
      g.metrics(CollectionContext(Map())) must equal(expected)

    }

    "remove expired values" in {
      val params = GaugeParams("/", expireAfter = 1.second)
      val g = new ConcreteGauge(params)
      g.set(4, Map("foo" -> "a"))
      g.set(5, Map("foo" -> "b"))
      g.tick(500.milliseconds)
      g.set(6, Map("foo" -> "b"))
      g.tick(501.milliseconds)

      val expected = Map(
        MetricAddress.Root -> Map(Map("foo" -> "b") -> SumValue(6L))
      )
      g.metrics(CollectionContext(Map())) must equal(expected)
    }

  }
      


}


