package colossus
package core

import testkit._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

import akka.testkit.TestProbe
import akka.util.ByteString

import scala.concurrent.duration._

class ConnectionSpec extends ColossusSpec with MockitoSugar{


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

