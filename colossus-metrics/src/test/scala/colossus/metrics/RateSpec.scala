package colossus.metrics

import org.scalatest._

import scala.concurrent.duration._
class RateSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  "Basic Rate" must {
    "tick" in {
      val r = new BasicRate(2.seconds)
      r.hit()
      r.hit()
      r.hit()
      r.value must equal(0)
      r.tick(1.second)
      r.value  must equal(0)
      r.tick(1.second)
      r.value must equal(3)

      r.tick(2.seconds)
      r.value must equal(0)
    }

  }

}

