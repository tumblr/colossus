package colossus.streaming

import colossus.testkit._
import scala.util.{Failure, Success}
import scala.concurrent.duration._

class MultiplexingSpec extends ColossusSpec {

  implicit val cbe = FakeIOSystem.testExecutor

  "Pipe Demultiplexer" must {
    import StreamComponent._
    import colossus.service.Callback

    case class FooFrame(id: Int, component: StreamComponent, value: Int)
    implicit object FooStream extends MultiStream[Int, FooFrame] {
      def streamId(f: FooFrame)  = f.id
      def component(f: FooFrame) = f.component
    }

    "start a new stream" in {
      val s = new BufferedPipe[FooFrame](5)

      val dem: Source[SubSource[Int, FooFrame]] = Multiplexing.demultiplex(s)

      s.push(FooFrame(1, Head, 1))

      val x: Callback[Int] = dem.pull() match {
        case PullResult.Item(sub) => sub.stream.fold(0) { case (build, next) => next + build.value }
        case other                => throw new Exception(s"wrong value $other")
      }
      s.push(FooFrame(1, Body, 2))
      s.push(FooFrame(1, Body, 3))
      s.push(FooFrame(1, Tail, 4))
      CallbackAwait.result(x, 1.second) mustBe 10
    }

    "demultiplex" in {
      val s = new BufferedPipe[FooFrame](5)

      val dem: Source[SubSource[Int, FooFrame]] = Multiplexing.demultiplex(s)
      var results                               = Map[Int, Int]()
      dem.map { sub =>
        sub.stream.fold(0) { case (build, next) => next + build.value }.execute {
          case Success(v) => results += (sub.id -> v)
          case Failure(e) => throw e
        }
      } into Sink.blackHole

      s.push(FooFrame(1, Head, 1))
      s.push(FooFrame(2, Head, 1))
      s.push(FooFrame(1, Body, 1))
      s.push(FooFrame(2, Tail, 1))
      s.push(FooFrame(1, Tail, 1))

      results mustBe Map(1 -> 3, 2 -> 2)
    }

    "terminating the base terminates demultiplexed pipe and all substreams" in {
      val s = new BufferedPipe[FooFrame](5)

      val dem: Source[SubSource[Int, FooFrame]] = Multiplexing.demultiplex(s)
      s.push(FooFrame(1, Head, 1))
      s.push(FooFrame(2, Head, 1))
      val PullResult.Item(SubSource(_, s1)) = dem.pull().asInstanceOf[PullResult.Item[SubSource[Int, FooFrame]]]
      val PullResult.Item(SubSource(_, s2)) = dem.pull().asInstanceOf[PullResult.Item[SubSource[Int, FooFrame]]]
      s1.outputState mustBe TransportState.Open
      s2.outputState mustBe TransportState.Open
      s.terminate(new Exception("BYE"))
      dem.outputState mustBe a[TransportState.Terminated]
      s1.outputState mustBe a[TransportState.Terminated]
      s2.outputState mustBe a[TransportState.Terminated]
    }

    "closing the base pipe closes the demultiplexed pipe and terminates all substreams" in {
      val s                                     = new BufferedPipe[FooFrame](5)
      val dem: Source[SubSource[Int, FooFrame]] = Multiplexing.demultiplex(s)
      s.push(FooFrame(1, Head, 1))
      val PullResult.Item(SubSource(_, s1)) = dem.pull().asInstanceOf[PullResult.Item[SubSource[Int, FooFrame]]]
      s1.outputState mustBe TransportState.Open
      s.complete()
      dem.outputState mustBe TransportState.Closed
      s1.outputState mustBe a[TransportState.Terminated]
    }

    "base reacts to backpressure from a substream" in {
      val s                                     = new BufferedPipe[FooFrame](2)
      val dem: Source[SubSource[Int, FooFrame]] = Multiplexing.demultiplex(s, substreamBufferSize = 1)
      s.push(FooFrame(1, Head, 1))
      val PullResult.Item(SubSource(_, s1)) = dem.pull().asInstanceOf[PullResult.Item[SubSource[Int, FooFrame]]]
      s.push(FooFrame(1, Body, 2)) mustBe PushResult.Ok
      s.push(FooFrame(1, Body, 3)) mustBe PushResult.Ok
      val m = s1.map { _.value }
      m.pull() mustBe PullResult.Item(1)
      m.pull() mustBe PullResult.Item(2)
      m.pull() mustBe PullResult.Item(3)
    }

  }

  "Pipe Multiplexer" must {

    case class FooFrame(id: Int, component: StreamComponent, value: Int)
    implicit object FooStream extends MultiStream[Int, FooFrame] {
      def streamId(f: FooFrame)  = f.id
      def component(f: FooFrame) = f.component
    }

    def multiplexed: Pipe[SubSource[Int, FooFrame], FooFrame] = {
      val base                                    = new BufferedPipe[FooFrame](5)
      val mplexed: Sink[SubSource[Int, FooFrame]] = Multiplexing.multiplex(base)
      new Channel(mplexed, base)
    }

    def foo(id: Int): Pipe[Int, FooFrame] = new BufferedPipe[Int](10).map { i =>
      FooFrame(id, StreamComponent.Body, i)
    }

    "basic multiplexing" in {
      val mplexed = multiplexed
      val s1      = foo(1)
      val s2      = foo(2)

      s1.push(3)

      mplexed.push(SubSource(1, s1)) mustBe PushResult.Ok
      mplexed.push(SubSource(2, s2)) mustBe PushResult.Ok

      s2.push(5)
      s1.push(7)

      mplexed.pull() mustBe PullResult.Item(FooFrame(1, StreamComponent.Body, 3))
      mplexed.pull() mustBe PullResult.Item(FooFrame(2, StreamComponent.Body, 5))
      mplexed.pull() mustBe PullResult.Item(FooFrame(1, StreamComponent.Body, 7))
    }

    "closing multiplexing sink with no open substreams immediately closes multiplexed pipe" in {
      val base                                    = new BufferedPipe[FooFrame](5)
      val mplexed: Sink[SubSource[Int, FooFrame]] = Multiplexing.multiplex(base)
      mplexed.complete()
      base.outputState mustBe TransportState.Closed
    }

    "closing the multiplexing sink keeps the mplexed open until all active substreams are finished" in {
      val mplexed = multiplexed

      val s1 = foo(1)
      val s2 = foo(2)
      mplexed.push(SubSource(1, s1)) mustBe PushResult.Ok
      mplexed.push(SubSource(2, s2)) mustBe PushResult.Ok
      mplexed.complete()
      mplexed.outputState mustBe TransportState.Open

      s1.push(7)
      mplexed.pull() mustBe PullResult.Item(FooFrame(1, StreamComponent.Body, 7))

      s1.complete()
      mplexed.outputState mustBe TransportState.Open
      s2.complete()
      mplexed.outputState mustBe TransportState.Closed
    }

    "terminating a substream doesn't fuck up the multiplexed stream" in {
      val mplexed = multiplexed
      val s1      = foo(1)
      val s2      = foo(2)
      mplexed.push(SubSource(1, s1))
      mplexed.push(SubSource(2, s2))

      s1.terminate(new Exception("AHHH FUCK"))
      s2.push(5)
      mplexed.pull() mustBe PullResult.Item(FooFrame(2, StreamComponent.Body, 5))
    }

    "terminated substreams are accurately accounted for in closing a multiplexed stream" in {
      val mplexed = multiplexed
      val s1      = foo(1)
      val s2      = foo(2)
      mplexed.push(SubSource(1, s1))
      mplexed.push(SubSource(2, s2))

      s1.terminate(new Exception("AHHH FUCK"))
      mplexed.complete()
      s2.complete()
      mplexed.outputState mustBe TransportState.Closed

    }

    "terminating the multiplexing sink fucks up everything" in {
      val base                                    = new BufferedPipe[FooFrame](5)
      val mplexed: Sink[SubSource[Int, FooFrame]] = Multiplexing.multiplex(base)
      val s1                                      = foo(1)
      mplexed.push(SubSource(1, s1))
      mplexed.terminate(new Exception("I need to return some video tapes"))
      s1.push(3) mustBe a[PushResult.Error]
    }

    "terminating the multiplexed pipe fucks up everything" taggedAs (org.scalatest.Tag("test")) in {
      val base                                    = new BufferedPipe[FooFrame](5)
      val mplexed: Sink[SubSource[Int, FooFrame]] = Multiplexing.multiplex(base)
      val s1                                      = foo(1)
      mplexed.push(SubSource(1, s1))
      base.terminate(new Exception("I need to return some video tapes"))
      s1.push(3) mustBe a[PushResult.Error]
      mplexed.inputState mustBe a[TransportState.Terminated]
    }

    "closing the multiplexed pipe fucks up everything" in {
      val base                                    = new BufferedPipe[FooFrame](5)
      val mplexed: Sink[SubSource[Int, FooFrame]] = Multiplexing.multiplex(base)
      val s1                                      = foo(1)
      mplexed.push(SubSource(1, s1))
      base.complete()
      s1.push(3) mustBe a[PushResult.Error]
      mplexed.inputState mustBe a[TransportState.Terminated]
    }

    "backpressure is correctly handled on the base stream" in {
      val mplexed = multiplexed
      val s1      = foo(1)
      val s2      = foo(2)
      mplexed.push(SubSource(1, s1))
      mplexed.push(SubSource(2, s2))

      (1 to 6).foreach(i => s1.push(i) mustBe PushResult.Ok)
      (1 to 6).foreach(i => s2.push(i) mustBe PushResult.Ok)

      (1 to 12).foreach { i =>
        val p = mplexed.pull()
        p mustBe a[PullResult.Item[_]]
      }
    }

  }

}
