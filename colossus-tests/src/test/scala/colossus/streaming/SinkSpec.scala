package colossus.streaming

import colossus.testkit._

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

  "Sink.open" must {
    "do what it's supposed to" in {
      val s = Sink.open[Int](i => PushResult.Ok)

      s.push(3) mustBe PushResult.Ok
      s.complete()
      s.terminate(new Exception("WAT"))
      s.inputState mustBe TransportState.Open
      s.pushPeek mustBe PushResult.Ok
    }
  }

  "Sink.mapIn" must {
    "map items" in {
      val s            = new BufferedPipe[String](4)
      val t: Sink[Int] = s.mapIn[Int](i => i.toString)
      t.push(4) mustBe PushResult.Ok
      s.pull mustBe PullResult.Item("4")
      t.complete()
      s.pull mustBe PullResult.Closed
      t.terminate(new Exception("WASDF"))
      s.pull mustBe a[PullResult.Error]
    }
  }

}
