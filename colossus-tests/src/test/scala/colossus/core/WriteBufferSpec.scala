package colossus
package core

import testkit._
import org.scalatest.mock.MockitoSugar

import akka.testkit.TestProbe
import akka.util.ByteString

import WriteStatus._

class WriteBufferSpec extends ColossusSpec {
/*

  class FakeWriteBuffer(val internalBufferSize: Int, channelBufferSize: Int = Int.MaxValue) extends WriteBuffer {
  
    protected var writes = collection.mutable.Queue[ByteString]()
    
    protected var clearCalled = false
    def onBufferClear() {
      clearCalled = true
    }

    def channelWrite(data: DataBuffer): Int = {
      val d = if (data.remaining <= channelBufferSize) ByteString(data.takeAll) else ByteString(data.take(channelBufferSize))
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

  */

  def data(s: String) = DataBuffer(ByteString(s))


  "WriteBuffer" must {
    "buffer some data and clear it" in {
      val b = new MockWriteBuffer(20)
      b.write(data("hello world")) must equal(Complete)
      b.expectOneWrite(ByteString("hello world"))
    }

    "buffer and drain data too large for internal buffer" in {
      val b = new MockWriteBuffer(4)
      b.write(data("1234567890")) must equal(Partial)
      b.expectOneWrite(ByteString("1234"))
      b.clearBuffer()
      b.continueWrite() must equal(false)
      b.expectOneWrite(ByteString("5678"))
      b.clearBuffer()
      b.continueWrite() must equal(true)
      b.expectOneWrite(ByteString("90"))
    }

    /*
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

    */



  }
}

