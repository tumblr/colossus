package colossus.streaming

import colossus.testkit._
import scala.util.{Success, Failure}

import scala.concurrent.duration._

import PushResult._

class PipeSpec extends ColossusSpec {

  implicit val cbe = FakeIOSystem.testExecutor

  implicit val duration = 1.second

  val PA = PullAction

  "BufferedPipe" must {

    "push an item after pull request without buffering" in {
      val pipe = new BufferedPipe[Int](1)
      var v    = 0
      pipe.pull { x =>
        v = x.get.get
      }
      pipe.push(2) mustBe Ok
      v must equal(2)
    }

    "push an item after pull request with another request during execution" in {
      val pipe = new BufferedPipe[Int](1)
      var v    = 0
      def pl() {
        pipe.pull { x =>
          v = x.get.get; pl()
        }
      }
      pl()
      pipe.push(2) must equal(Ok)
      v must equal(2)
    }

    "reject pushes after pipe is closed" in {
      val pipe = new BufferedPipe[Int](1)
      pipe.pull { x =>
        pipe.complete()
      }
      pipe.push(2) must equal(Ok)
      pipe.push(3) mustBe Closed
    }

    "fail to push when pipe is full" in {
      val pipe = new BufferedPipe[Int](1)
      pipe.push(1) mustBe PushResult.Ok
      pipe.push(1) mustBe an[Full]
    }

    "received Close if pipe is closed outside pull execution" in {
      val pipe = new BufferedPipe[Int](1)
      pipe.complete()
      pipe.push(1) must equal(Closed)
    }

    "fail to push when pipe has been terminated" in {
      val pipe = new BufferedPipe[Int](1)
      pipe.terminate(new Exception("sadfsadf"))
      pipe.push(1) mustBe an[Error]
    }

    "full trigger fired when pipe opens up" in {
      val pipe = new BufferedPipe[Int](1)
      pipe.push(8)
      val f = pipe.push(1) match {
        case Full(trig) => trig
        case other      => fail(s"didn't get trigger, got $other")
      }
      var triggered = false
      f.notify { triggered = true }
      triggered must equal(false)
      pipe.pull { _ =>
        ()
      }
      triggered must equal(true)
    }

    "immediately fail pulls when terminated" in {
      val pipe = new BufferedPipe[Int](1)
      pipe.terminate(new Exception("asdf"))
      var pulled = false
      pipe.pull { r =>
        pulled = true; r mustBe an[Failure[_]]
      }
      pulled mustBe true
    }

    "fold" in {
      val pipe     = new BufferedPipe[Int](1)
      var executed = false
      pipe.fold(0) { case (a, b) => a + b }.execute {
        case Success(6) => { executed = true }
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
      val pipe     = new BufferedPipe[Int](1)
      var executed = false
      pipe
        .foldWhile(0) { (a, b) =>
          a + b
        } { _ != 10 }
        .execute {
          case Success(x) if x < 10 => { executed = true }
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
      val pipe     = new BufferedPipe[Int](1)
      var executed = false
      pipe
        .foldWhile(0) { (a, b) =>
          a + b
        } { _ != 10 }
        .execute {
          case Success(10) => { executed = true }
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

    "handle multiple push triggers" in {
      val pipe = new BufferedPipe[Int](1)
      def tryPush(i: Int): Unit = pipe.push(i) match {
        case PushResult.Full(t) =>
          t.notify {
            println(s"triggered $i");
            pipe.push(i) match {
              case PushResult.Ok => ()
              case other         => throw new Exception(s"wrong notify result $other")
            }
          }
        case other => throw new Exception(s"wrong pushresult $other")
      }
      pipe.push(0) //fill the pipe
      tryPush(1)
      tryPush(2)
      pipe.pull() mustBe PullResult.Item(0)
      pipe.pull() mustBe PullResult.Item(1)
      pipe.pull() mustBe PullResult.Item(2)
    }

    "buffer until full" in {
      val pipe = new BufferedPipe[Int](3)
      pipe.push(1) mustBe Ok
      pipe.push(1) mustBe Ok
      pipe.push(1) mustBe Ok
      pipe.push(1) mustBe a[Full]
      pipe.complete()
      CallbackAwait.result(pipe.reduce { _ + _ }, 1.second) mustBe 3
    }

    "enqueue and dequeue in the right order" in {
      val pipe = new BufferedPipe[Int](10)
      pipe.push(1) mustBe Ok
      pipe.push(2) mustBe Ok
      pipe.push(3) mustBe Ok
      val c = pipe.fold((0, true)) { case (next, (last, ok)) => (next, next > last && ok) }
      pipe.complete()
      CallbackAwait.result(c, 1.second) mustBe (3, true)
    }

  }

  "BufferedPipe.pullWhile" must {

    "fast-track pushes" in {
      val p   = new BufferedPipe[Int](5)
      var sum = 0
      p.pullWhile(
        i => {
          sum += i
          if (i == 1) PA.PullContinue else PA.PullStop
        },
        _ => ()
      )
      p.push(1)
      sum mustBe 1
      p.push(2)
      sum mustBe 3
      p.push(1)
      sum mustBe 3
    }

    "fast-track: callback is notified of closing" in {
      val p      = new BufferedPipe[Int](5)
      var closed = false
      p.pullWhile(
        _ => PA.PullContinue, {
          case PullResult.Closed => closed = true
          case _                 => {}
        }
      )
      p.push(1)
      closed mustBe false
      p.complete()
      closed mustBe true
    }

    "trigger pushes on emptying a full buffer" in {
      val p = new BufferedPipe[Int](2)
      p.push(1) mustBe PushResult.Ok
      p.push(2) mustBe PushResult.Ok
      p.push(3) match {
        case PushResult.Full(signal) => signal.notify { p.push(4) }
        case _                       => throw new Exception("WRONG")
      }
      var sum = 0
      p.pullWhile(
        i => {
          sum += i
          PA.PullContinue
        },
        _ => PA.Stop
      )
      sum mustBe 7
    }

    "drain the buffer when fast-tracking" in {
      val p = new BufferedPipe[Int](10)
      p.push(1)
      p.push(2)
      p.push(3)
      var sum = 0
      p.pullWhile(
        i => {
          sum += i
          PA.PullContinue
        },
        _ => ()
      )
      sum mustBe 6
      p.push(1)
      sum mustBe 7
    }

    "properly terminate when Terminate action is returned" in {
      val p = new BufferedPipe[Int](10)
      p.push(1)
      var terminated = false
      p.pullWhile(
        i => PullAction.Terminate(new Exception("HEY")), {
          case PullResult.Error(reason) => terminated = true
          case _                        => {}
        }
      )
      terminated mustBe true
      p.outputState mustBe a[TransportState.Terminated]
    }

  }

  "BufferedPipe.peek" must {
    "return Item" in {
      val p = new BufferedPipe[Int](3)
      p.peek mustBe a[PullResult.Empty]
      p.push(2)
      p.peek mustBe PullResult.Item(2)
    }
    "return item after being closed until empty" in {
      val p = new BufferedPipe[Int](3)
      p.push(2)
      p.complete()
      p.peek mustBe PullResult.Item(2)
      p.pull()
      p.peek mustBe PullResult.Closed
    }
    "return Terminated" in {
      val p = new BufferedPipe[Int](3)
      p.terminate(new Exception("WAT"))
      p.peek mustBe a[PullResult.Error]
    }
  }

  "BufferedPipe.pullUntilNull" must {

    def setup(onNotify: => Unit): BufferedPipe[Int] = {
      val p = new BufferedPipe[Int](2)
      p.push(1) mustBe PushResult.Ok
      p.push(2) mustBe PushResult.Ok
      p.push(3) match {
        case PushResult.Full(signal) => signal.notify { onNotify }
        case _                       => throw new Exception("WRONG")
      }
      p
    }

    "set off signals when previously full" in {
      var sum                       = 0
      lazy val p: BufferedPipe[Int] = setup(p.push(3))
      p.pullUntilNull({ x =>
          sum += x; true
        })
        .get mustBe a[PullResult.Empty]
      sum mustBe 6
    }

    "return Closed if push trigger closes" in {
      var sum                       = 0
      lazy val p: BufferedPipe[Int] = setup(p.complete())
      p.pullUntilNull({ x =>
        sum += x; true
      }) mustBe Some(PullResult.Closed)
      sum mustBe 3
    }

    "return Error if terminated in push trigger" in {
      lazy val p: BufferedPipe[Int] = setup(p.terminate(new Exception("ASDF")))
      p.pullUntilNull(_ => true).get mustBe a[PullResult.Error]
    }

    "return Error if pipe is terminated while pulling" in {
      val p = new BufferedPipe[Int](10)
      (0 to 3).foreach(p.push)
      p.pullUntilNull(i => { if (i > 1) p.terminate(new Exception("ADSF")); true }).get mustBe a[PullResult.Error]
    }

  }

  "Pipe" must {
    "map" in {
      val p = new BufferedPipe[Int](5)
      val q = p.map { _.toString }
      q.push(4)
      q.pull() mustBe PullResult.Item("4")
    }

  }

  "Pipe.fuse" must {
    "weld two pipes" in {
      val p1                    = new BufferedPipe[Int](1)
      val p2                    = new BufferedPipe[String](5)
      val p3: Pipe[Int, String] = p1 map { _.toString } weld p2
      p3.push(1) mustBe PushResult.Ok
      p3.push(2) mustBe PushResult.Ok
      p3.pull() mustBe PullResult.Item("1")
      p3.pull() mustBe PullResult.Item("2")
    }
  }

  "Pipe.filterMap" must {
    "filter and map correctly" in {
      val p = new BufferedPipe[Int](10).filterMap { i =>
        if (i > 3) Some(i.toString) else None
      }
      p.push(4)
      p.push(2)
      p.pull() mustBe PullResult.Item("4")
      p.pull() mustBe a[PullResult.Empty]
    }
  }

  "Pipe.filterScan" must {
    "remove buffered items" in {
      val p = new BufferedPipe[Int](10)
      (1 to 4).foreach(p.push)
      p.filterScan(_ % 2 == 0)
      p.pull() mustBe PullResult.Item(1)
      p.pull() mustBe PullResult.Item(3)
      p.pull() mustBe a[PullResult.Empty]

    }

    "correctly trigger push signals when at least one item is removed" in {
      val p = new BufferedPipe[Int](2)
      p.push(1)
      p.push(2)
      var notified = false
      p.push(3) match {
        case PushResult.Full(signal) => signal.notify { notified = true }
        case _                       => throw new Exception("WRONG RESULT")
      }
      p.filterScan(_ > 5)
      notified mustBe false
      p.filterScan(_ == 1)
      notified mustBe true
      p.pull() mustBe PullResult.Item(2)

    }

  }

  "channel" must {
    "correctly link a source and sink" in {
      val p1                         = new BufferedPipe[Int](4)
      val p2                         = new BufferedPipe[String](4)
      val channel: Pipe[Int, String] = new Channel(p1, p2)
      channel.push(4) mustBe PushResult.Ok
      p1.pull() mustBe PullResult.Item(4)

      p2.push("HELLO") mustBe PushResult.Ok
      channel.pull() mustBe PullResult.Item("HELLO")
    }

    "create two linked pipes" in {
      val (a, b) = Channel[Int, String]()
      a.push(3) mustBe PushResult.Ok
      b.pull() mustBe PullResult.Item(3)

      b.push("hello") mustBe PushResult.Ok
      a.pull() mustBe PullResult.Item("hello")
    }
  }

}
