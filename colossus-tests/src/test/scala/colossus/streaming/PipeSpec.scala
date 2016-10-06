package colossus
package streaming

import colossus.testkit._
import scala.util.{Success, Failure}

import scala.concurrent.duration._

import PushResult._

class PipeSpec extends ColossusSpec {

  implicit val cbe = FakeIOSystem.testExecutor

  implicit val duration = 1.second

  "BufferedPipe" must {

    "push an item after pull request without buffering" in {
      val pipe = new BufferedPipe[Int](1)
      var v = 0
      pipe.pull{x => v = x.get.get}
      pipe.push(2) mustBe Ok
      v must equal(2)
    }

    "push an item after pull request with another request during execution" in {
      val pipe = new BufferedPipe[Int](1)
      var v = 0
      def pl() {
        pipe.pull{x => v = x.get.get;pl()}
      }
      pl()
      pipe.push(2) must equal(Ok)
      v must equal(2)
    }

    "pull an item immediately while triggering a push signal" in {
      val pipe = new BufferedPipe[Int](1)
      pipe.push(1) match {
        case PushResult.Filled(sig) => sig.notify{pipe.push(1);()}
        case _ => throw new Exception("wrong push result")
      }
      pipe.pull() mustBe PullResult.Item(1)
    }

    "reject pushes after pipe is closed" in {
      val pipe = new BufferedPipe[Int](1)
      pipe.pull{x => pipe.complete()}
      pipe.push(2) must equal(Ok)
      pipe.push(3) mustBe Closed
    }


    "fail to push when pipe is full" in {
      val pipe = new BufferedPipe[Int](1)
      pipe.push(1) mustBe an[Filled]
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
      val f = pipe.push(1) match {
        case Filled(trig) => trig
        case other => fail(s"didn't get trigger, got $other")
      }
      var triggered = false
      f.notify{triggered = true}
      triggered must equal(false)
      pipe.pull{_ => ()}
      triggered must equal(true)
    }

    "immediately fail pulls when terminated" in {
      val pipe = new BufferedPipe[Int](1)
      pipe.terminate(new Exception("asdf"))
      var pulled = false
      pipe.pull{r => pulled = true; r mustBe an[Failure[_]]}
      pulled mustBe true
    }

    "fold" in {
      val pipe = new BufferedPipe[Int](1)
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
      val pipe = new BufferedPipe[Int](1)
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
      val pipe = new BufferedPipe[Int](1)
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
      val pipe = new BufferedPipe[Int](1)
      def tryPush(i: Int): Unit = pipe.push(i) match {
        case PushResult.Full(t) => t.notify{ println(s"triggered $i");pipe.push(i) match { 
          case PushResult.Ok => ()
          case PushResult.Filled(_) => ()
          case other => throw new Exception(s"wrong notify result $other")
        }}
        case other => throw new Exception(s"wrong pushresult $other")
      }
      pipe.push(0)//fill the pipe
      tryPush(1)
      tryPush(2)
      pipe.pull() mustBe PullResult.Item(0)
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



  "Pipe" must {
    "map" in {
      val p = new BufferedPipe[Int](5)
      val q = p.map{_.toString}
      q.push(4)
      q.pull() mustBe PullResult.Item("4")
    }

    "filterScan" in {
      val p = new BufferedPipe[Int](10)
      (1 to 4).foreach(p.push)
      p.filterScan(_ % 2 == 0)
      p.pull() mustBe PullResult.Item(1)
      p.pull() mustBe PullResult.Item(3)
      p.pull() mustBe a[PullResult.Empty]
    }

  }

  "Pipe.fuse" must {
    "weld two pipes" in {
      val p1 = new BufferedPipe[Int](1)
      val p2 = new BufferedPipe[String](5)
      val p3: Pipe[Int, String] = p1 map {_.toString} weld p2
      p3.push(1) mustBe PushResult.Ok
      p3.push(2) mustBe PushResult.Ok
      p3.pull() mustBe PullResult.Item("1")
      p3.pull() mustBe PullResult.Item("2")
    }
  }



  "channel" must {
    "correctly link a source and sink" in {
      val p1 = new BufferedPipe[Int](4)
      val p2 = new BufferedPipe[String](4)
      val channel: Pipe[Int, String] = new Channel(p1, p2)
      channel.push(4) mustBe PushResult.Ok
      p1.pull() mustBe PullResult.Item(4)

      p2.push("HELLO") mustBe PushResult.Ok
      channel.pull() mustBe PullResult.Item("HELLO")
    }

    "create two linked pipes" in {
      val (a,b) = Channel[Int, String]()
      a.push(3) mustBe PushResult.Ok
      b.pull() mustBe PullResult.Item(3)

      b.push("hello") mustBe PushResult.Ok
      a.pull() mustBe PullResult.Item("hello")
    }
  }

}

