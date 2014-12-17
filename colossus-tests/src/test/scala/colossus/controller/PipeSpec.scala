package colossus
package controller

import org.scalatest._

import akka.util.ByteString
import core.DataBuffer
import scala.util.{Try, Success, Failure}
import Copier._

class PipeSpec extends WordSpec with MustMatchers {

  implicit object IntCopier extends Copier[Int] {
    def copy(i: Int) = i
  }
  
  "InfinitePipe" must {

    "push an item" in {
      val pipe = new InfinitePipe[Int](10)
      pipe.push(2) must equal(Success(PushResult.Ok))
    }

    "fail to push when pipe is full" in {
      val pipe = new InfinitePipe[Int](2)
      pipe.push(1) must equal(Success(PushResult.Ok))
      pipe.push(1) mustBe an[Success[PushResult.Full]]
      pipe.push(1) mustBe an[Failure[PushResult]]
    }

    "fail to push when pipe has been completed" in {
      val pipe = new InfinitePipe[Int](2)
      pipe.push(1) must equal(Success(PushResult.Ok))
      pipe.complete()
      pipe.push(1) mustBe an[Failure[PushResult]]
    }

    "fail to push when pipe has been terminated" in {
      val pipe = new InfinitePipe[Int](2)
      pipe.push(1) must equal(Success(PushResult.Ok))
      pipe.terminate(new Exception("sadfsadf"))
      pipe.push(1) mustBe an[Failure[PushResult]]
    }

    "pull an item already pushed" in {
      val pipe = new InfinitePipe[Int](20)
      pipe.push(1) must equal(Success(PushResult.Ok))
      var pulled = false
      pipe.pull{
        case Success(Some(item)) => pulled = true
        case o => throw new Exception(s"wrong result $o")
      }
      pulled must equal(true)
    }

    "pull an item before it is pushed" in {
      val pipe = new InfinitePipe[Int](20)
      var pulled = false
      pipe.pull{
        case Success(Some(item)) => pulled = true
        case o => throw new Exception(s"wrong result $o")
      }
      pulled must equal(false)
      pipe.push(1) must equal(Success(PushResult.Ok))
      pulled must equal(true)
    }

    "pull items in the same order they were pushed" in {
      val pipe = new InfinitePipe[Int](20)
      var pulled: Vector[Int] = Vector()
      (0 to 5).foreach{i =>
        pipe.push(i)
      }
      (0 to 5).foreach{i =>
        pipe.pull{
          case Success(Some(i)) => pulled = pulled :+ i
          case _ => throw new Exception("wat")
        }
      }
      pulled.toList must equal((0 to 5).toList)
    }

    "full trigger fired when pipe opens up" in {
      val pipe = new InfinitePipe[Int](2)
      pipe.push(1) must equal(Success(PushResult.Ok))
      val r = pipe.push(1) 
      var triggered = false
      r mustBe an[Success[PushResult.Full]]
      r.asInstanceOf[Success[PushResult.Full]].get.trigger.fill{() => triggered = true}
      pipe.pull{_ => ()}
      triggered must equal(true)
    }

    "continue to pull items until empty after completed" in {
      val pipe = new InfinitePipe[Int](20)
      pipe.push(1) must equal(Success(PushResult.Ok))
      pipe.push(2) must equal(Success(PushResult.Ok))
      pipe.complete()
      var pulled1 = false
      var pulled2 = false
      var pulled3 = false
      pipe.pull{r => pulled1 = true; r must equal(Success(Some(1)))}
      pipe.pull{r => pulled2 = true; r must equal(Success(Some(2)))}
      pipe.pull{r => pulled3 = true; r must equal(Success(None))}
    }

    "immediately fail pulls when terminated" in {
      val pipe = new InfinitePipe[Int](20)
      pipe.push(1) must equal(Success(PushResult.Ok))
      pipe.push(2) must equal(Success(PushResult.Ok))
      pipe.terminate(new Exception("asdf"))
      var pulled = false
      pipe.pull{r => pulled = true; r mustBe an[Failure[Int]]}
    }

    "get a callback from pullCB" in {
      val pipe = new InfinitePipe[Int](20)
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

    "fold" in {
      val pipe = new InfinitePipe[Int](20)
      pipe.push(1)
      var executed = false
      pipe.fold(0){case (a, b) => a + b}.execute{
        case Success(6) => {executed = true}
        case other => {
          println("FAIL")
          throw new Exception(s"bad end value $other")
        }

      }
      executed must equal(false)
      pipe.push(2)
      executed must equal(false)
      pipe.push(3)
      pipe.complete()
      executed must equal(true)
    }

        

      

  }

  "FiniteBytePipe" must {
    "close after correct number of bytes have been pushed" in {
      val pipe = new FiniteBytePipe(7, 4)
      val data = DataBuffer(ByteString("1234567890"))
      pipe.push(data) must equal(Success(PushResult.Done))
      data.remaining must equal(3)
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

