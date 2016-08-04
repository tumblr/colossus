package colossus
package core

import colossus.metrics.MetricSystem
import testkit._

import scala.concurrent.duration._

class ConnectionLimiterSpec extends ColossusSpec {


  "connection limiter" must {
    class ManualLimiter(var manualTime: Long, initial: Int, max: Int, duration: FiniteDuration) extends ConnectionLimiter(initial, max, duration) {
      override def currentTime = manualTime
    }

    "correctly increase limit to max" in {
      val l = new ManualLimiter(0, 1, 100, 5.seconds)
      l.limit mustBe 2
      l.manualTime = 1000
      l.limit mustBe 4
      l.manualTime = 2000
      l.limit mustBe 8
      l.manualTime = 2500
      l.limit mustBe 16
      l.manualTime = 3000
      l.limit mustBe 32
      l.manualTime = 4000
      l.limit mustBe 64
      l.manualTime = 5000
      l.limit mustBe 100
      l.manualTime = 1000000
      l.limit mustBe 100
    }

    "no limiting for nolimiter" in {
      val l = ConnectionLimiter.noLimiting(50)
      l.limit mustBe(50)
    }

  }

}


