package colossus
package controller

import colossus.testkit.{FakeIOSystem, ColossusSpec, CallbackMatchers}
import org.scalatest._

import akka.util.ByteString
import core.DataBuffer
import scala.util.{Success, Failure}

import scala.concurrent.duration._

import PushResult._

class PipeSpec extends ColossusSpec with MustMatchers with CallbackMatchers {

  implicit val cbe = FakeIOSystem.testExecutor

  implicit val duration = 1.second

  "BufferedPipe" must {

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

    "receive Complete result when puller completes pipe during execution" in {
      val pipe = new BufferedPipe[Int](0)
      pipe.pull{x => pipe.complete()}
      pipe.push(2) must equal(Complete)
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
      f.fill{() => triggered = true}
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
  }


}

