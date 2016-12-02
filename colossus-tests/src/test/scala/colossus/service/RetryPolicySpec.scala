package colossus.core

import colossus.testkit._
import scala.concurrent.duration._

import RetryAttempt._
import BackoffMultiplier._

class RetryPolicySpec extends ColossusSpec {

  "Multipliers" must {

    "constant" in {
      val b = Constant
      b.value(1.millisecond, 1) must equal(1.millisecond)
      b.value(1.millisecond, 10) must equal(1.millisecond)
      b.value(1.millisecond, 100) must equal(1.millisecond)
    }

    "linear" in {
      val b = Linear(50.milliseconds)
      b.value(1.millisecond, 1) must equal(1.millisecond)
      b.value(1.millisecond, 32) must equal(32.millisecond)
      b.value(1.millisecond, 51) must equal(50.millisecond)
    }


    "exponential" in {
      val b = Exponential(50.milliseconds)
      b.value(1.millisecond, 1) must equal(1.millisecond)
      b.value(1.millisecond, 2) must equal(2.millisecond)
      b.value(1.millisecond, 3) must equal(4.millisecond)
      b.value(1.millisecond, 4) must equal(8.millisecond)
      b.value(1.millisecond, 7) must equal(50.milliseconds)
    }

  }


  "BackoffPolicy" must {

    "respect max attempts" in {
      val p = BackoffPolicy(10.milliseconds, Exponential(1.second), maxTries = Some(3), immediateFirstAttempt = false)
      val i = p.start()
      i.nextAttempt() must equal(RetryIn(10.milliseconds))
      i.nextAttempt() must equal(RetryIn(20.milliseconds))
      i.nextAttempt() must equal(RetryIn(40.milliseconds))
      i.nextAttempt() must equal(Stop)
    }

    "respect max time" in {
      val p = BackoffPolicy(20.milliseconds, Exponential(1.second), maxTime = Some(50.milliseconds), immediateFirstAttempt = false)
      val i = p.start()
      i.nextAttempt() must equal(RetryIn(20.milliseconds))
      i.nextAttempt() must equal(RetryIn(40.milliseconds))
      i.nextAttempt() must equal(RetryIn(80.milliseconds))
      i.nextAttempt() must equal(RetryIn(160.milliseconds))
      i.nextAttempt() must equal(RetryIn(320.milliseconds))
      Thread.sleep(60)

      i.nextAttempt() must equal(Stop)
    }

    "not cause overlow after enough attempts" in {
      val p = BackoffPolicy(20.milliseconds, Exponential(1.second), maxTime = Some(50.milliseconds), immediateFirstAttempt = false)
      val i = p.start()
      (1 to 50).foreach{_ =>
        i.nextAttempt()
      }
      i.nextAttempt() must equal(RetryIn(1.second))
    }



  }

}
