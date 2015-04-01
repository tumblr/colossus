package colossus.metrics

import org.scalatest._

import MetricAddress.Root

import MetricValues._

class MetricValueSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  "WeightedAverageValue" must {
    "combine two non-zero values" in {
      val a = WeightedAverageValue(value = 2, weight = 10)
      val b = WeightedAverageValue(value = 5, weight = 5)
      val expected = WeightedAverageValue(value = 3, weight = 15)

      a + b must equal(expected)
    }

    "combine non-zero with zero value" in {
      val a = WeightedAverageValue(value = 4, weight = 10)
      val b = WeightedAverageValue(value = 0, weight = 0)
      val expected = WeightedAverageValue(value = 4, weight = 10)
      a + b must equal(expected)
    }

    "handle two zero weight values" in {
      val a = WeightedAverageValue(0, 0)
      a + a must equal(a)
    }

  }

}

