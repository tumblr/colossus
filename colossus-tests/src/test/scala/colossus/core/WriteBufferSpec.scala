package colossus
package core

import testkit._
import org.scalatest.mock.MockitoSugar

import akka.testkit.TestProbe
import akka.util.ByteString

import WriteStatus._

class WriteBufferSpec extends ColossusSpec {

  class FakeWriteBuffer(val internalBufferSize: Int) extends WriteBuffer {
  
    protected var writes = collection.mutable.Queue[ByteString]()
    
    protected var clearCalled = false
    def onBufferClear() {
      clearCalled = true
    }

    def channelWrite(data: DataBuffer): Int = {
      val d = ByteString(data.takeAll)
      writes.enqueue(d)
      d.size
    }

    def expectChannelWrite(expected: String) {
      writes.dequeue must equal(ByteString(expected))
    }

    def expectClearCalled() {
      clearCalled must equal(true)
    }
    def expectNoClearCalled() {
      clearCalled must equal(false)
    }

    protected def setKeyInterest() {}

  }

  def data(s: String) = DataBuffer(ByteString(s))


  "WriteBuffer" must {
    "buffer some data and clear it" in {
      val b = new FakeWriteBuffer(20)
      b.write(data("hello")) must equal(Complete)
      b.write(data(" world")) must equal(Complete)
      b.handleWrite()
      b.expectChannelWrite("hello world")
      b.expectNoClearCalled()
    }

    "properly set write key interest" in {
      val b = new FakeWriteBuffer(20)
      b.writeReadyEnabled must equal(false)
      b.write(data("hello"))
      b.writeReadyEnabled must equal(true)
      b.handleWrite()
      b.writeReadyEnabled must equal(false)
    }

    "buffer and drain data too large for internal buffer" in {
      val b = new FakeWriteBuffer(4)
      b.write(data("1234567890")) must equal(Partial)
      b.handleWrite()
      b.expectChannelWrite("1234")
      b.writeReadyEnabled must equal(true)
      b.handleWrite()
      b.expectChannelWrite("5678")
      b.handleWrite()
      b.expectChannelWrite("90")
      b.writeReadyEnabled must equal(false)
    }

    "reject more writes while draining when buffer filled" in {
      val b = new FakeWriteBuffer(4)
      b.write(data("12345678")) must equal(Partial)
      b.write(data("90")) must equal(Zero)
    }
    
    "allow writes after buffer drained" in {
      val b = new FakeWriteBuffer(4)
      b.write(data("12345678")) must equal(Partial)
      b.handleWrite()
      b.expectChannelWrite("1234")
      b.write(data("90")) must equal(Partial)
      b.handleWrite()
      b.expectChannelWrite("5678")
      b.write(data("ab")) must equal(Complete)
      b.handleWrite()
      b.expectChannelWrite("90ab")
    }

    "immediately call disconnect callback when no data buffered" in {
      val b = new FakeWriteBuffer(4)
      b.write(data("hello"))
      b.handleWrite()
      b.handleWrite()
      b.writeReadyEnabled must equal(false)
      var called = false
      b.disconnectBuffer(() => {called = true})
      called must equal(true)
    }

    "call disconnect callback when internal buffer drained" in {
      val b = new FakeWriteBuffer(4)
      b.write(data("12345678"))
      var called = false
      b.disconnectBuffer(() => {called = true})
      called must equal(false)
      b.handleWrite()
      called must equal(false)
      b.handleWrite()
      called must equal(true)
    }

    "disallow more writes after disconnect callback has been set" in {
      val b = new FakeWriteBuffer(4)
      b.disconnectBuffer(() => ())
      b.write(data("asf")) must equal(Failed)
    }

    "properly catch CancelledKeyException on key set interest in writes" in {
      class FailFakeWriter extends FakeWriteBuffer(10) {
        override def setKeyInterest() {
          throw new java.nio.channels.CancelledKeyException()
        }
      }

      val b = new FailFakeWriter
      b.write(data("asdfsadf")) must equal(Failed)
    }



  }
}

