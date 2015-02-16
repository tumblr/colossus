package colossus
package controller

import colossus.testkit.CallbackMatchers
import org.scalatest._

import akka.util.ByteString
import core.DataBuffer
import scala.util.{Success, Failure}

import PushResult._

class PipeSpec extends WordSpec with MustMatchers with CallbackMatchers {

  implicit object IntCopier extends Copier[Int] {
    def copy(i: Int) = i
  }
  
  "InfinitePipe" must {

    "push an item after pull request" in {
      val pipe = new InfinitePipe[Int]
      var v = 0
      pipe.pull{x => v = x.get.get}
      pipe.push(2) mustBe an[Filled]
      v must equal(2)
    }

    "push an item after pull request with another request during execution" in {
      val pipe = new InfinitePipe[Int]
      var v = 0
      def pl() {
        pipe.pull{x => v = x.get.get;pl()}
      }
      pl()
      pipe.push(2) must equal(Ok)
      v must equal(2)
    }

    "receive Complete result when puller completes pipe during execution" in {
      val pipe = new InfinitePipe[Int]
      pipe.pull{x => pipe.complete()}
      pipe.push(2) must equal(Complete)
    }


    "fail to push when pipe is full" in {
      val pipe = new InfinitePipe[Int]
      pipe.push(1) mustBe an[Full]
    }

    "received Close if pipe is closed outside pull execution" in {
      val pipe = new InfinitePipe[Int]
      pipe.complete()
      pipe.push(1) must equal(Closed)
    }

    "fail to push when pipe has been terminated" in {
      val pipe = new InfinitePipe[Int]
      pipe.terminate(new Exception("sadfsadf"))
      pipe.push(1) mustBe an[Error]
    }

    "full trigger fired when pipe opens up" in {
      val pipe = new InfinitePipe[Int]
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
      val pipe = new InfinitePipe[Int]
      pipe.terminate(new Exception("asdf"))
      var pulled = false
      pipe.pull{r => pulled = true; r mustBe an[Failure[_]]}
      pulled mustBe true
    }

    /*
    "get a callback from pullCB" in {
      val pipe = new InfinitePipe[Int]
      pipe.push(1) must equal(Success(PushResult.Ok))
      pipe.push(2) must equal(Success(PushResult.Ok))
      var ok = false
      val cb = pipe.pullCB().map{
        case Some(1) => ok = true
        case _ => throw new Exception("failed!!")
      }
      ok must equal(false)
      cb.execute()
      ok must equal(true)
    }
    */

    "fold" in {
      val pipe = new InfinitePipe[Int]
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
      val pipe = new InfinitePipe[Int]
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
      val pipe = new InfinitePipe[Int]
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

  "FiniteBytePipe" must {
    "close after correct number of bytes have been pushed" in {
      val pipe = new FiniteBytePipe(7)
      val data = DataBuffer(ByteString("1234567890"))
      pipe.fold(ByteString){(a, b) => b}.execute()
      pipe.push(data) must equal(Complete)
      data.remaining must equal(3)
    }

    "not allow negatively sized pipes" in {
      intercept[IllegalArgumentException] {
        new FiniteBytePipe(-1)
      }
    }

    "immediately close 0 sized pipes" in {
      val pipe = new FiniteBytePipe(0)
      val data = DataBuffer(ByteString("1234567890"))
      pipe.push(data) must equal(Closed)
      data.remaining must equal(10)
      //this works..because the CB will never fire if the pipe is not closed
      pipe.pullCB() must evaluateTo { x : Option[DataBuffer]=>
        x must be (None)
      }

    }

    "properly calculates bytes remaining" taggedAs(org.scalatest.Tag("test")) in {
      //arose from a bug where the pipe was using the buffer's total length
      //instead of how many bytes were readable
      val pipe = new FiniteBytePipe(4)
      val data = DataBuffer(ByteString("1234567"))
      var res = ByteString()
      data.take(5)
      pipe.fold(ByteString()){(buf, build) => build ++ ByteString(buf.takeAll)}.execute{
        case Success(bstr) => res = bstr
        case Failure(err) => throw err
      }
      pipe.push(data) must equal(Ok)
      val data2 = DataBuffer(ByteString("abcdefg"))
      pipe.push(data2) must equal(Complete)
      res must equal(ByteString("67ab"))
    }

  }

  /*
  "DualPipe" must {
    "combine two pipes" in {
      val p1 = new FiniteBytePipe(10, 5)
      val p2 = new FiniteBytePipe(10, 5)
      val p3 = p1 ++ p2
      p1.push(DataBuffer(ByteString("12345")))
      p1.push(DataBuffer(ByteString("67890")))
      p2.push(DataBuffer(ByteString("abcde")))
      var res: String = ""
      def drain
      */


}

