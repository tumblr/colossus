package colossus
package streaming

import colossus.testkit._

import scala.util.{Success, Failure}

import scala.concurrent.duration._


class SinkSpec extends ColossusSpec {

  implicit val cbe = FakeIOSystem.testExecutor

  "Sink.blackhole" must {
    "act like a sink" in {
      val b = Sink.blackHole[Int]
      b.push(3) mustBe PushResult.Ok
      b.inputState mustBe TransportState.Open
      b.complete()
      b.push(5) mustBe PushResult.Closed
      b.inputState mustBe TransportState.Closed

      val c = Sink.blackHole[Int]
      c.terminate(new Exception("FUCK"))
      c.push(4) mustBe a[PushResult.Error]
      c.inputState mustBe a[TransportState.Terminated]
    }
  }

}

