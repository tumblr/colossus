package colossus.streaming

import colossus.testkit._

class CircuitBreakerSpec extends ColossusSpec {

  def setup(bufferSize: Int = 1, onBreak: Throwable => Any = _ => ()): (PipeCircuitBreaker[Int, Int], Pipe[Int, Int]) = {
    val p = new PipeCircuitBreaker[Int, Int](onBreak)
    val b = new BufferedPipe[Int](bufferSize)
    p.set(b)
    (p, b)
  }

  "CircuitBreaker" must {
    "return no-action results when no pipe" in {
      val p = new PipeCircuitBreaker[Int, Int]
      p.push(3) mustBe a[PushResult.Full]
      p.pull mustBe a[PullResult.Empty]
    }
    "trigger both push and pull signals when set" in {
      val p            = new PipeCircuitBreaker[Int, Int]
      var pushsignaled = false
      var pullsignaled = true
      p.push(1) match {
        case PushResult.Full(signal) => signal.notify { pushsignaled = true }
        case _                       => throw new Exception("WRONG PUSH RESULT")
      }
      p.pull() match {
        case PullResult.Empty(signal) => signal.notify { pullsignaled = true }
        case _                        => throw new Exception("WRONG PULL RESULT")
      }
      p.set(new BufferedPipe[Int](3))
      pushsignaled mustBe true
      pullsignaled mustBe true
    }

    "close unsets" in {
      val (p, b) = setup()
      p.complete()
      p.inputState mustBe TransportState.Open
      b.inputState mustBe TransportState.Closed
      p.push(1) mustBe a[PushResult.Full]
    }

    "terminate unsets" in {
      val (p, b) = setup()
      p.terminate(new Exception("WAT"))
      p.inputState mustBe TransportState.Open
      b.inputState mustBe a[TransportState.Terminated]
      p.push(1) mustBe a[PushResult.Full]
    }

    "termination of inner pipe is completely contained" in {
      val (p, b) = setup()

      val (d, b2) = setup()

      p into d
      p.push(1) mustBe PushResult.Ok
      d.pull() mustBe PullResult.Item(1)

      b.terminate(new Exception("UH OH"))
      d.isSet mustBe true

      val n = new BufferedPipe[Int](1)
      p.set(n)
      d.set(new BufferedPipe[Int](1))
      p.push(3) mustBe PushResult.Ok
      //we expect the previous "into" to be broken now
      d.pull() mustBe PullResult.Item(3)
    }

    "unsetting a circuitbreaker does not sever link" in {
      val (c, _) = setup()
      val b      = new BufferedPipe[Int](1)

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

    "redirect internal pipe Closed on Pull" in {
      var error: Option[Throwable] = None
      val (cb, pipe)               = setup(onBreak = err => error = Some(err))
      pipe.complete()
      error mustBe None
      cb.pull() mustBe a[PullResult.Empty]
      error.get mustBe a[InternalTransportClosedException]
      cb.isSet mustBe false
    }

    "redirect internal pipe Closed on Push" in {
      var error: Option[Throwable] = None
      val (cb, pipe)               = setup(onBreak = err => error = Some(err))
      pipe.complete()
      error mustBe None
      cb.push(1) mustBe a[PushResult.Full]
      error.get mustBe a[InternalTransportClosedException]
      cb.isSet mustBe false
    }

    "redirect internal terminated on Pull" in {
      var error: Option[Throwable] = None
      val (cb, pipe)               = setup(onBreak = err => error = Some(err))
      val exception                = new Exception("bah")
      pipe.terminate(exception)
      error mustBe None
      cb.pull() mustBe a[PullResult.Empty]
      error.get mustBe a[PipeTerminatedException]
      cb.isSet mustBe false

    }

    "redirect internal terminated on Push" in {
      var error: Option[Throwable] = None
      val (cb, pipe)               = setup(onBreak = err => error = Some(err))
      val exception                = new Exception("bah")
      pipe.terminate(exception)
      error mustBe None
      cb.push(1) mustBe a[PushResult.Full]
      error.get mustBe a[PipeTerminatedException]
      cb.isSet mustBe false

    }

    "properly redirect closed on pullWhile" in {
      var error: Option[Throwable] = None
      val (cb, pipe)               = setup(onBreak = err => error = Some(err))
      var sum                      = 0
      cb.pullWhile({
        case n => { sum += n; PullAction.PullContinue }
      }, _ => throw new Exception("should not be called"))
      cb.push(2)
      sum mustBe 2
      pipe.complete
      error.isDefined mustBe true
      cb.isSet mustBe false
      cb.set(new BufferedPipe[Int](1))
      cb.push(3) mustBe PushResult.Ok
      sum mustBe 5
    }

    "properly redirect closed on pullUntilNull" in {
      var error: Option[Throwable] = None
      val (cb, pipe)               = setup(onBreak = err => error = Some(err))
      var sum                      = 0
      pipe.complete()
      cb.pullUntilNull(_ => true).get mustBe a[PullResult.Empty]
      cb.isSet mustBe false
    }

  }

}
