package colossus.core

import colossus.testkit.{ColossusSpec, MockWriteBuffer}
import akka.util.ByteString
import WriteStatus._

class WriteBufferSpec extends ColossusSpec {

  def data(s: String) = DataBuffer(ByteString(s))

  "WriteBuffer" must {
    "buffer some data and clear it" in {
      val b = new MockWriteBuffer(20)
      b.testWrite(data("hello world")) must equal(Complete)
      b.expectOneWrite(ByteString("hello world"))
    }

    "buffer and drain data too large for internal buffer" in {
      val b = new MockWriteBuffer(4)
      b.testWrite(data("1234567890")) must equal(Partial)
      b.expectOneWrite(ByteString("1234"))
      b.clearBuffer()
      b.continueWrite() must equal(false)
      b.expectOneWrite(ByteString("5678"))
      b.clearBuffer()
      b.continueWrite() must equal(true)
      b.expectOneWrite(ByteString("90"))
    }

    "immediately call disconnect callback when no data buffered" in {
      val b = new MockWriteBuffer(4)
      b.testWrite(data("hell"))
      b.status must equal(ConnectionStatus.Connected)
      b.gracefulDisconnect()
      println(b.isDataBuffered)
      b.status must equal(ConnectionStatus.NotConnected)
    }

    "call disconnect callback only when all data is written" in {
      val b = new MockWriteBuffer(4)
      b.testWrite(data("12345678"))
      b.gracefulDisconnect()
      b.status must equal(ConnectionStatus.Connected)
      b.clearBuffer()
      b.continueWrite() must equal(true)
      b.status must equal(ConnectionStatus.NotConnected)
    }

    "disallow more writes after disconnect callback has been set" in {
      val b = new MockWriteBuffer(4)
      b.gracefulDisconnect()
      b.testWrite(data("asf")) must equal(Failed)
    }

  }
}
