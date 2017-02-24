package colossus.streaming

import colossus.testkit._

class CircuitBreakerSpec extends ColossusSpec {

  "CircuitBreaker" must {
    "return no-action results when no pipe" in {
      val p = new PipeCircuitBreaker[Int, Int]
      p.push(3) mustBe a[PushResult.Full]
      p.pull mustBe a[PullResult.Empty]
    }
    "trigger both push and pull signals when set" in {
      val p = new PipeCircuitBreaker[Int, Int]
      var pushsignaled = false
      var pullsignaled = true
      p.push(1) match {
        case PushResult.Full(signal) => signal.notify{pushsignaled = true}
        case _ => throw new Exception("WRONG PUSH RESULT")
      }
      p.pull() match {
        case PullResult.Empty(signal) => signal.notify{pullsignaled = true}
        case _ => throw new Exception("WRONG PULL RESULT")
      }
      p.set(new BufferedPipe[Int](3))
      pushsignaled mustBe true
      pullsignaled mustBe true
    }

    "close unsets" in {
      val p = new PipeCircuitBreaker[Int, Int]
      val b = new BufferedPipe[Int](1)
      p.set(b)
      p.complete()
      p.inputState mustBe TransportState.Open
      b.inputState mustBe TransportState.Closed
      p.push(1) mustBe a [PushResult.Full]
    }

    "terminate unsets" in {
      val p = new PipeCircuitBreaker[Int, Int]
      val b = new BufferedPipe[Int](1)
      p.set(b)
      p.terminate(new Exception("WAT"))
      p.inputState mustBe TransportState.Open
      b.inputState mustBe a[TransportState.Terminated]
      p.push(1) mustBe a [PushResult.Full]
    }

    "termination of inner pipe propagates across linked circuitbreakers" in {
      val p = new PipeCircuitBreaker[Int, Int]
      val b = new BufferedPipe[Int](1)
      p.set(b)

      val b2 = new BufferedPipe[Int](1)
      val d = new PipeCircuitBreaker[Int, Int]
      d.set(b2)

      p into d
      p.push(1) mustBe PushResult.Ok
      d.pull() mustBe PullResult.Item(1)

      b.terminate(new Exception("UH OH"))
      d.isSet mustBe false

      val n = new BufferedPipe[Int](1)
      p.set(n)
      d.set(new BufferedPipe[Int](1))
      p.push(3) mustBe PushResult.Ok
      //we expect the previous "into" to be broken now
      d.pull() mustBe a[PullResult.Empty]
    }

    "unsetting a circuitbreaker does not sever link" in {
      val c = new PipeCircuitBreaker[Int, Int]
      val b = new BufferedPipe[Int](1)
      c.set(new BufferedPipe[Int](1))

      b into c
      b.push(1) mustBe PushResult.Ok
      c.pull() mustBe PullResult.Item(1)

      //because the termination happens after unsetting the circuit-breaker, the
      //upstream pipe will have it's signal triggered, but on its next attempt
      //will get a Full Result from the circuit-break instead of an Error from
      //the terminated pipe
      c.unset().get.terminate(new Exception("Uh Oh"))

      b.push(2) mustBe PushResult.Ok
      b.push(3) mustBe a[PushResult.Full]

      c.set(new BufferedPipe[Int](5))
      c.pull() mustBe PullResult.Item(2)
    }

  }

}

