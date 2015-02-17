package colossus
package core

import testkit._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

import akka.testkit.TestProbe
import akka.util.ByteString

import scala.concurrent.duration._

class ConnectionSpec extends ColossusSpec with MockitoSugar{

  "WriteBuffer" must {
    "write some bytes" in {
      val m = new MockWriteBuffer(10)
      m.write(DataBuffer(ByteString("hello"))) must equal (WriteStatus.Complete)
      m.bytesAvailable must equal(5)
      m.readsEnabled must equal(true)
      m.writeReadyEnabled must equal(false)
      m.expectBufferNotCleared
      m.expectOneWrite(ByteString("hello"))
    }

    "partially buffer large data on overwrite" in {
      val m = new MockWriteBuffer(10)
      m.write(DataBuffer(ByteString("a1a2a3a4a5b1b2b3b4b5"))) must equal(WriteStatus.Partial)
      m.bytesAvailable must equal(0)
      m.readsEnabled must equal(true)
      m.writeReadyEnabled must equal(true)
      m.expectBufferNotCleared()
      m.expectOneWrite(ByteString("a1a2a3a4a5"))
      m.clearBuffer()
      m.readsEnabled must equal(true)
      m.writeReadyEnabled must equal(false)
      m.expectBufferCleared()
      m.expectOneWrite(ByteString("b1b2b3b4b5"))
    }

    "reject write calls when data is partially buffered" in {
      val m = new MockWriteBuffer(10)
      m.write(DataBuffer(ByteString("a1a2a3a4a5b1b2b3b4b5"))) must equal(WriteStatus.Partial)
      m.write(DataBuffer(ByteString("waaaatttt"))) must equal(WriteStatus.Zero)
    }
    

  }

  "ClientConnection" must {
    "timeout idle connection" in {
      val channel = mock[java.nio.channels.SocketChannel]
      val key     = mock[java.nio.channels.SelectionKey]
      val handler = mock[ClientConnectionHandler]
      when(handler.maxIdleTime).thenReturn(100.milliseconds)
      val con = new ClientConnection(1, key, channel, handler)
      val time = System.currentTimeMillis
      con.isTimedOut(time) must equal(false)
      con.isTimedOut(time + 101) must equal(true)
      Thread.sleep(30)
      con.write(DataBuffer(ByteString("asdf")))
      val time2 = System.currentTimeMillis
      con.isTimedOut(time + 101) must equal(false)
      con.isTimedOut(time2 + 101) must equal(true)
      Thread.sleep(30)
      val time3 = System.currentTimeMillis
      con.handleRead(DataBuffer(ByteString("WHATEVER")))(time3)
      con.isTimedOut(time2 + 101) must equal(false)
      con.isTimedOut(time3 + 101) must equal(true)
    }
  }

}

