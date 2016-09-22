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
      f.react{triggered = true}
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

    "can't pull twice" in {
      val pipe = new BufferedPipe(0)
      pipe.pull(_ => ())
      intercept[PipeStateException] {
        CallbackAwait.result(pipe.pullCB, 1.second)
      }
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


    "return Filling past highWatermark" in {
      val pipe = new BufferedPipe[Int](5, highWatermarkP = 0.5)
      pipe.push(1) mustBe Ok
      pipe.push(1) mustBe Ok
      pipe.push(1) mustBe Filling
      pipe.push(1) mustBe Filling
      pipe.push(1) mustBe a[Filled]
    }

    "execute trigger only on hitting low watermark" in {
      val pipe = new BufferedPipe[Int](5, highWatermarkP = 1.0, lowWatermarkP = 0.5)
      (1 to 4).foreach{_ => 
        pipe.push(1)
      }
      val t: Signal = pipe.push(1) match {
        case Filled(trig) => trig
        case other => throw new Exception(s"Got $other instead of full")
      }
      var triggered = false
      t.react{triggered = true}
      pipe.pull(_ => ())
      pipe.pull(_ => ())
      triggered mustBe false
      pipe.pull(_ => ())
      triggered mustBe true
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

  "Source.fromIterator" must {

    "read a full iterator" in {
      val items = Array(1, 2, 3, 4, 5, 6, 7, 8)
      val s: Source[Int] = Source.fromIterator(items.toIterator)
      s.isClosed mustBe false
      CallbackAwait.result(s.reduce{_ + _}, 1.second) mustBe items.sum
      s.isClosed mustBe true
    }

    "terminate before reading everything" in {
      val s = Source.fromIterator(Array(1, 2, 3, 4).toIterator)
      CallbackAwait.result(s.pullCB, 1.second) mustBe Some(1)
      s.terminated mustBe false
      s.terminate(new Exception("HI"))
      s.terminated mustBe true
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
      val a = Source.fromIterator(Array(1, 2, 3, 4).toIterator)
      val b = Source.fromIterator(Array(5, 6, 7, 8).toIterator)
      val c = (a ++ b)
      a.terminate(new Exception("A"))
      c.terminated mustBe true
      intercept[Exception] {
        CallbackAwait.result(c.pullCB, 1.second)
      }
    }

    "terminating second source only terminates dual when first source is empty" in {
      val a = Source.fromIterator(Array(1).toIterator)
      val b = Source.fromIterator(Array(5, 6).toIterator)
      val c = (a ++ b)
      b.terminate(new Exception("B"))
      c.terminated mustBe false
      c.pull(_ => ())
      intercept[Exception] {
        CallbackAwait.result(c.pullCB, 1.second)
      }
      c.terminated mustBe true

    }

    "terminating the dual terminates both sources" in {
      val a = Source.fromIterator(Array(1, 2, 3, 4).toIterator)
      val b = Source.fromIterator(Array(5, 6, 7, 8).toIterator)
      val c = (a ++ b)
      c.terminate(new Exception("C"))
      a.terminated mustBe true
      b.terminated mustBe true
    }
      
  }






}

