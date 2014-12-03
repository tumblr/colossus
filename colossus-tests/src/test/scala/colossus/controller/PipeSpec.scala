package colossus
package controller

import org.scalatest._

import akka.util.ByteString
import core.DataBuffer
import Copier._

class PipeSpec extends WordSpec with MustMatchers {

  implicit object IntCopier extends Copier[Int] {
    def copy(i: Int) = i
  }
  
  "InfinitePipe" must {

    "push an item" in {
      val pipe = new InfinitePipe[Int](10)
      pipe.push(2) must equal(PushResult.Ok)
    }

    "fail to push when pipe is full" in {
      val pipe = new InfinitePipe[Int](2)
      pipe.push(1) must equal(PushResult.Ok)
      pipe.push(1) mustBe an[PushResult.Full]
      pipe.push(1) mustBe an[PushResult.Error]
    }

    "fail to push when pipe has been completed" in {
      val pipe = new InfinitePipe[Int](2)
      pipe.push(1) must equal(PushResult.Ok)
      pipe.complete()
      pipe.push(1) mustBe an[PushResult.Error]
    }

    "fail to push when pipe has been terminated" in {
      val pipe = new InfinitePipe[Int](2)
      pipe.push(1) must equal(PushResult.Ok)
      pipe.terminate(new Exception("sadfsadf"))
      pipe.push(1) mustBe an[PushResult.Error]
    }

    "pull an item already pushed" in {
      val pipe = new InfinitePipe[Int](20)
      pipe.push(1) must equal(PushResult.Ok)
      var pulled = false
      pipe.pull{
        case PullResult.Item(item) => pulled = true
        case o => throw new Exception(s"wrong result $o")
      }
      pulled must equal(true)
    }

    "pull an item before it is pushed" in {
      val pipe = new InfinitePipe[Int](20)
      var pulled = false
      pipe.pull{
        case PullResult.Item(item) => pulled = true
        case o => throw new Exception(s"wrong result $o")
      }
      pulled must equal(false)
      pipe.push(1) must equal(PushResult.Ok)
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
          case PullResult.Item(i) => pulled = pulled :+ i
          case _ => throw new Exception("wat")
        }
      }
      pulled.toList must equal((0 to 5).toList)
    }

    "full trigger fired when pipe opens up" in {
      val pipe = new InfinitePipe[Int](2)
      pipe.push(1) must equal(PushResult.Ok)
      val r = pipe.push(1) 
      var triggered = false
      r mustBe an[PushResult.Full]
      r.asInstanceOf[PushResult.Full].trigger.fill{() => triggered = true}
      pipe.pull{_ => ()}
      triggered must equal(true)
    }

    "continue to pull items until empty after completed" in {
      val pipe = new InfinitePipe[Int](20)
      pipe.push(1) must equal(PushResult.Ok)
      pipe.push(2) must equal(PushResult.Ok)
      pipe.complete()
      var pulled1 = false
      var pulled2 = false
      var pulled3 = false
      pipe.pull{r => pulled1 = true; r must equal(PullResult.Item(1))}
      pipe.pull{r => pulled2 = true; r must equal(PullResult.Item(2))}
      pipe.pull{r => pulled3 = true; r must equal(PullResult.End)}
    }

    "immediately fail pulls when terminated" in {
      val pipe = new InfinitePipe[Int](20)
      pipe.push(1) must equal(PushResult.Ok)
      pipe.push(2) must equal(PushResult.Ok)
      pipe.terminate(new Exception("asdf"))
      var pulled = false
      pipe.pull{r => pulled = true; r mustBe an[PullResult.Error]}
    }


  }

  "FiniteBytePipe" must {
    "close after correct number of bytes have been pushed" in {
      val pipe = new FiniteBytePipe(7, 4)
      val data = DataBuffer(ByteString("1234567890"))
      pipe.push(data) must equal(PushResult.Done)
      data.remaining must equal(3)
    }
  }

}

