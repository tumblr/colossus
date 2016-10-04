package colossus
package controller

import colossus.testkit._
import org.scalatest._

import akka.util.ByteString
import core.DataBuffer
import scala.util.{Success, Failure}

import scala.concurrent.duration._

import PushResult._

class PipeSpec extends ColossusSpec with MustMatchers with CallbackMatchers {

  implicit val cbe = FakeIOSystem.testExecutor

  implicit val duration = 1.second

  "BufferedPipe (no buffering)" must {

    "push an item after pull request without buffering" in {
      val pipe = new BufferedPipe[Int](0)
      var v = 0
      pipe.pull{x => v = x.get.get}
      pipe.push(2) mustBe Ok
      v must equal(2)
    }

    "push an item after pull request with another request during execution" in {
      val pipe = new BufferedPipe[Int](0)
      var v = 0
      def pl() {
        pipe.pull{x => v = x.get.get;pl()}
      }
      pl()
      pipe.push(2) must equal(Ok)
      v must equal(2)
    }

    "pull an item immediately while triggering a push signal" in {
      val pipe = new BufferedPipe[Int](0)
      pipe.push(1) match {
        case PushResult.Full(sig) => sig.react{_ => pipe.push(1);()}
        case _ => throw new Exception("wrong push result")
      }
      pipe.pull() mustBe PullResult.Item(1)
    }

    "reject pushes after pipe is closed" in {
      val pipe = new BufferedPipe[Int](0)
      pipe.pull{x => pipe.complete()}
      pipe.push(2) must equal(Ok)
      pipe.push(3) mustBe Closed
    }


    "fail to push when pipe is full" in {
      val pipe = new BufferedPipe[Int](0)
      pipe.push(1) mustBe an[Full]
    }

    "received Close if pipe is closed outside pull execution" in {
      val pipe = new BufferedPipe[Int](0)
      pipe.complete()
      pipe.push(1) must equal(Closed)
    }

    "fail to push when pipe has been terminated" in {
      val pipe = new BufferedPipe[Int](0)
      pipe.terminate(new Exception("sadfsadf"))
      pipe.push(1) mustBe an[Error]
    }

    "full trigger fired when pipe opens up" in {
      val pipe = new BufferedPipe[Int](0)
      val f = pipe.push(1) match {
        case Full(trig) => trig
        case other => fail(s"didn't get trigger, got $other")
      }
      var triggered = false
      f.react{_ => triggered = true}
      triggered must equal(false)
      pipe.pull{_ => ()}
      triggered must equal(true)
    }

    "immediately fail pulls when terminated" in {
      val pipe = new BufferedPipe[Int](0)
      pipe.terminate(new Exception("asdf"))
      var pulled = false
      pipe.pull{r => pulled = true; r mustBe an[Failure[_]]}
      pulled mustBe true
    }

    "fold" in {
      val pipe = new BufferedPipe[Int](0)
      var executed = false
      pipe.fold(0){case (a, b) => a + b}.execute{
        case Success(6) => {executed = true}
        case other => {
          println("FAIL")
          throw new Exception(s"bad end value $other")
        }

      }
      executed must equal(false)
      pipe.push(1) must equal(Ok)
      pipe.push(2) must equal(Ok)
      executed must equal(false)
      pipe.push(3) must equal(Ok)
      pipe.complete()
      executed must equal(true)
    }

    "foldWhile folds before terminal condition is met" in {
      val pipe = new BufferedPipe[Int](0)
      var executed = false
      pipe.foldWhile(0){(a, b) => a + b}{_ != 10}.execute{
        case Success(x) if x < 10 => {executed = true}
        case other => {
          throw new Exception(s"bad end value $other")
        }

      }
      //the first 4 pushes should bring our total to 8
      (1 to 4).foreach { i =>
        executed must equal(false)
        pipe.push(2) must equal(Ok)
      }

      pipe.complete()
      executed must equal(true)
    }

    "foldWhile stops folding after terminal condition" in {
      val pipe = new BufferedPipe[Int](0)
      var executed = false
      pipe.foldWhile(0){(a, b) => a + b}{_ != 10}.execute{
        case Success(10) => {executed = true}
         case other => {
          throw new Exception(s"bad end value $other")
         }
      }
       //the first 4 pushes should bring our total to 8
       (1 to 4).foreach { i =>
         executed must equal(false)
         pipe.push(2)
       }
        (1 to 4).foreach { i =>
          pipe.push(2)
        }
        pipe.complete()
        executed must equal(true)
    }

    "handle multiple push triggers"  in {
      val pipe = new BufferedPipe[Int](0)
      def tryPush(i: Int): Unit = pipe.push(i) match {
        case PushResult.Full(t) => t.notify{ println(s"triggered $i");pipe.push(i) match { 
          case PushResult.Ok => ()
          case other => throw new Exception(s"wrong notify result $other")
        }}
        case other => throw new Exception(s"wrong pushresult $other")
      }
      tryPush(1)
      tryPush(2)
      pipe.pull() mustBe PullResult.Item(1)
      pipe.pull() mustBe PullResult.Item(2)
    }
  }

  "BufferedPipe (with buffering)" must {

    "buffer until full" in {
      val pipe = new BufferedPipe[Int](3)
      pipe.push(1) mustBe Ok
      pipe.push(1) mustBe Ok
      pipe.push(1) mustBe a[Filled]
      pipe.push(1) mustBe a[Full]
      pipe.complete()
      CallbackAwait.result(pipe.reduce{_ + _}, 1.second) mustBe 3
    }

    "enqueue and dequeue in the right order" in {
      val pipe = new BufferedPipe[Int](10)
      pipe.push(1) mustBe Ok
      pipe.push(2) mustBe Ok
      pipe.push(3) mustBe Ok
      val c = pipe.fold((0, true)){ case (next, (last, ok)) => (next, next > last && ok) }
      pipe.complete()
      CallbackAwait.result(c, 1.second) mustBe (3, true)
    }


    "fast-track pushes" in {
      val p = new BufferedPipe[Int](5)
      var sum = 0
      p.pullWhile{
        case PullResult.Item(i) => {
          sum += i
          if (i == 1) true else false
        }
        case _ => true
      }
      p.push(1)
      sum mustBe 1
      p.push(2)
      sum mustBe 3
      p.push(1)
      sum mustBe 3
    }

    "fast-track: callback is notified of closing" in {
      val p = new BufferedPipe[Int](5)
      var closed = false
      p.pullWhile{
        case PullResult.Closed => {
          closed = true
          false
        }
        case _ => true
      }
      p.push(1)
      closed mustBe false
      p.complete()
      closed mustBe true
    }



    "drain the buffer when fast-tracking" in {
      val p = new BufferedPipe[Int](10)
      p.push(1)
      p.push(2)
      p.push(3)
      var sum = 0
      p.pullWhile{
        case PullResult.Item(i) => {
          sum += i
          true
        }
        case _ => true
      }
      sum mustBe 6
      p.push(1)
      sum mustBe 7
    }

  }

  "Sink" must {
    "feed an iterator" in {
      val p = new BufferedPipe[Int](3)
      val items = Array(1, 2, 3, 4, 5, 6, 7, 8)
      p.feed(items.toIterator)
      CallbackAwait.result(p.reduce{_ + _}, 1.second) mustBe items.sum
    }
  }

  "Source" must {
    "map" in {
      import Types._
      import PipeOps._
      val s = Source.fromIterator(Array(1, 2).toIterator)
      val t = s.map{_.toString}
      t.pull() mustBe PullResult.Item("1")
    }
  }

  "Pipe" must {
    "map" in {
      import PipeOps._
      val p = new BufferedPipe[Int](5)
      val q = p.map{_.toString}
      q.push(4)
      q.pull() mustBe PullResult.Item("4")
    }
  }

  "Pipe.fuse" must {
    "weld two pipes" in {
      import PipeOps._
      val p1 = new BufferedPipe[Int](0)
      val p2 = new BufferedPipe[String](5)
      val p3: Pipe[Int, String] = p1 map {_.toString} weld p2
      p3.push(1) mustBe PushResult.Ok
      p3.push(2) mustBe PushResult.Ok
      p3.pull() mustBe PullResult.Item("1")
      p3.pull() mustBe PullResult.Item("2")
    }
  }

  "Source.into" must {
    "push items into sink"  in {
      val x = Source.fromIterator(Array(2, 4).toIterator)
      val y = new BufferedPipe[Int](0)
      x into y
      y.pull mustBe PullResult.Item(2)
      y.pull mustBe PullResult.Item(4)
      y.pull mustBe PullResult.Closed
    }
  }

  "Source.fromIterator" must {

    "read a full iterator" in {
      val items = Array(1, 2, 3, 4, 5, 6, 7, 8)
      val s: Source[Int] = Source.fromIterator(items.toIterator)
      s.outputState mustBe TransportState.Open
      CallbackAwait.result(s.reduce{_ + _}, 1.second) mustBe items.sum
      s.outputState mustBe TransportState.Closed
    }

    "terminate before reading everything" in {
      val s = Source.fromIterator(Array(1, 2, 3, 4).toIterator)
      CallbackAwait.result(s.pullCB, 1.second) mustBe Some(1)
      s.outputState mustBe TransportState.Open
      s.terminate(new Exception("HI"))
      s.outputState mustBe a[TransportState.Terminated]
      intercept[Exception] {
        CallbackAwait.result(s.pullCB, 1.second)
      }
    }

    "Source.one" in {
      val s: Source[Int] = Source.one(5)
      CallbackAwait.result(s.pullCB, 1.second) mustBe Some(5)
      CallbackAwait.result(s.pullCB, 1.second) mustBe None
    }

  }

  "DualSource" must {
    "correctly link two sources" in {
      val a = Source.fromIterator(Array(1, 2, 3, 4).toIterator)
      val b = Source.fromIterator(Array(5, 6, 7, 8).toIterator)

      
      val c = (a ++ b).fold((0, true)){ case (next, (last, ok)) => (next, next > last && ok) }
      CallbackAwait.result(c, 1.second) mustBe (8, true)
    }

    "terminating first source immediately terminates the dual" in {
      val x = Source.fromIterator(Array(1, 2, 3, 4).toIterator)
      val y = Source.fromIterator(Array(5, 6, 7, 8).toIterator)
      val c = (x ++ y)
      x.terminate(new Exception("A"))
      c.outputState mustBe a[TransportState.Terminated]
      intercept[Exception] {
        CallbackAwait.result(c.pullCB, 1.second)
      }
    }

    "terminating second source only terminates dual when first source is empty" in {
      val x = Source.fromIterator(Array(1).toIterator)
      val y = Source.fromIterator(Array(5, 6).toIterator)
      val c = (x ++ y)
      y.terminate(new Exception("B"))
      c.outputState mustBe TransportState.Open
      c.pull(_ => ())
      intercept[Exception] {
        CallbackAwait.result(c.pullCB, 1.second)
      }
      c.outputState mustBe a[TransportState.Terminated]

    }

    "terminating the dual terminates both sources" in {
      val x = Source.fromIterator(Array(1, 2, 3, 4).toIterator)
      val y = Source.fromIterator(Array(5, 6, 7, 8).toIterator)
      val c = (x ++ y)
      c.terminate(new Exception("C"))
      x.outputState mustBe a[TransportState.Terminated]
      y.outputState mustBe a[TransportState.Terminated]
    }
      
  }

  "Pipe Demultiplexer" must {
    import streaming._
    import StreamComponent._
    import service.Callback

    case class FooFrame(id: Int, component: StreamComponent, value: Int)
    implicit object FooStream extends MultiStream[Int, FooFrame] {
      def streamId(f: FooFrame) = f.id
      def component(f: FooFrame) = f.component
    }

    "start a new stream" in {
      val s = new BufferedPipe[FooFrame](5)
      
      val dem: Source[SubSource[Int, FooFrame]] = Multiplexing.demultiplex(s)

      s.push(FooFrame(1, Head, 1))

      val x: Callback[Int] = dem.pull() match {
        case PullResult.Item(sub) => sub.stream.fold(0){case (build, next) => next + build.value}
        case other => throw new Exception(s"wrong value $other")
      }
      s.push(FooFrame(1, Body, 2))
      s.push(FooFrame(1, Body, 3))
      s.push(FooFrame(1, Tail, 4))
      CallbackAwait.result(x, 1.second) mustBe 10
    }

    "demultiplex" in {
      import Types._
      import PipeOps._
      val s = new BufferedPipe[FooFrame](5)
      
      val dem: Source[SubSource[Int, FooFrame]] = Multiplexing.demultiplex(s)
      var results = Map[Int, Int]()
      dem.map{sub => sub.stream.fold(0){case (build, next) => next + build.value}.execute{ 
        case Success(v) => results += (sub.id -> v)
        case Failure(e) => throw e
      }} into Sink.blackHole

      s.push(FooFrame(1, Head, 1))
      s.push(FooFrame(2, Head, 1))
      s.push(FooFrame(1, Body, 1))
      s.push(FooFrame(2, Tail, 1))
      s.push(FooFrame(1, Tail, 1))

      results mustBe Map(1 -> 3, 2 -> 2)
    }
  }

  "Pipe Multiplexer" must {
    import streaming._
    import StreamComponent._
    import service.Callback
    import Types._
    import PipeOps._

    case class FooFrame(id: Int, component: StreamComponent, value: Int)
    implicit object FooStream extends MultiStream[Int, FooFrame] {
      def streamId(f: FooFrame) = f.id
      def component(f: FooFrame) = f.component
    }

    def multiplexed: Pipe[SubSource[Int, FooFrame], FooFrame] = {
      val base = new BufferedPipe[FooFrame](5)
      val mplexed: Sink[SubSource[Int, FooFrame]] = Multiplexing.multiplex(base)
      new Channel(mplexed, base)
    }

    def foo(id: Int): Pipe[Int, FooFrame] = new BufferedPipe[Int](10).map{i => FooFrame(id, StreamComponent.Body, i)}

    "basic multiplexing"  in {
      val mplexed = multiplexed
      val s1 = foo(1)
      val s2 = foo(2)

      s1.push(3)

      mplexed.push(SubSource(1, s1)) mustBe PushResult.Ok
      mplexed.push(SubSource(2, s2)) mustBe PushResult.Ok

      s2.push(5)
      s1.push(7)

      mplexed.pull() mustBe PullResult.Item(FooFrame(1, StreamComponent.Body, 3))
      mplexed.pull() mustBe PullResult.Item(FooFrame(2, StreamComponent.Body, 5))
      mplexed.pull() mustBe PullResult.Item(FooFrame(1, StreamComponent.Body, 7))
    }

    "closing the multiplexed sink keeps the mplexed open until all active substreams are finished"  in {
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
      val s1 = foo(1)
      val s2 = foo(2)
      mplexed.push(SubSource(1, s1))
      mplexed.push(SubSource(2, s2))

      s1.terminate(new Exception("AHHH FUCK"))
      s2.push(5)
      mplexed.pull() mustBe PullResult.Item(FooFrame(2, StreamComponent.Body, 5))
    }

    "terminated substreams are accurately accounted for in closing a multiplexed stream" in {
      val mplexed = multiplexed
      val s1 = foo(1)
      val s2 = foo(2)
      mplexed.push(SubSource(1, s1))
      mplexed.push(SubSource(2, s2))

      s1.terminate(new Exception("AHHH FUCK"))
      mplexed.complete()
      s2.complete()
      mplexed.outputState mustBe TransportState.Closed
      
    }

    "terminating the multiplexed stream fucks up everything" in {
      val mplexed = multiplexed
      val s1 = foo(1)
      mplexed.push(SubSource(1, s1))
      mplexed.terminate(new Exception("I need to return some video tapes"))
      s1.push(3) mustBe a[PushResult.Error]
    }

    "backpressure is correctly handled on the base stream" taggedAs(org.scalatest.Tag("test")) in {
      val mplexed = multiplexed
      val s1 = foo(1)
      val s2 = foo(2)
      mplexed.push(SubSource(1, s1))
      mplexed.push(SubSource(2, s2))

      (1 to 6).foreach(i => s1.push(i) mustBe PushResult.Ok)
      (1 to 6).foreach(i => s2.push(i) mustBe PushResult.Ok)

      (1 to 12).foreach{i =>
        val p = mplexed.pull() 
        println(s"got $p")
        p mustBe a[PullResult.Item[FooFrame]]
      }
    }

      

      
  }




      
      






}

