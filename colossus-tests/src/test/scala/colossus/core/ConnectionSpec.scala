package colossus
package core

import testkit._
import java.nio.channels.SocketChannel
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

import akka.testkit.TestProbe
import akka.util.ByteString

import scala.concurrent.duration._

class ConnectionSpec extends ColossusSpec with MockitoSugar{

  "Connection" must {

    "call handler.shutdownRequest during become" in {
      val handler = mock[ServerConnectionHandler]
      val con = MockConnection.server(handler)
      verify(handler, never()).shutdownRequest()
      con.become(throw new Exception("If this is thrown, the connection tried to instantiate the handler too early"))
      verify(handler).shutdownRequest()
    }

    "not set shutdown action of lower priority" in {
      import ShutdownAction._
      val con = MockConnection.server(mock[ServerConnectionHandler])
      con.setShutdownAction(DefaultDisconnect) must equal(true)
      con.setShutdownAction(Become(() => ???))  must equal(true)
      con.setShutdownAction(DefaultDisconnect) must equal(false)
      con.setShutdownAction(Disconnect) must equal(true)
      con.setShutdownAction(DefaultDisconnect) must equal(false)
      con.setShutdownAction(Become(() => ???))  must equal(false)
    }

    "catch exceptions thrown in handler's connectionTerminated when connection closed" in {
      val handler = new BasicSyncHandler with ClientConnectionHandler {

        override def connectionClosed(cause: DisconnectCause) {
          println("here")
          throw new Exception("x_x")
        }

        override def connectionLost(error: DisconnectError) {
          throw new Exception("o_O")
        }

        def receivedData(data: DataBuffer){}
      }
      val con = MockConnection.client(handler)

      //this test fails if this throws an exception
      con.close(DisconnectCause.Closed)
      con.close(DisconnectCause.Disconnect)
    }

  }


  "ClientConnection" must {
    "timeout idle connection" in {
      val handler = mock[ClientConnectionHandler]
      when(handler.maxIdleTime).thenReturn(100.milliseconds)
      val con = MockConnection.client(handler)
      val time = System.currentTimeMillis
      con.isTimedOut(time) must equal(false)
      con.isTimedOut(time + 101) must equal(true)
      Thread.sleep(30)
      con.testWrite(DataBuffer(ByteString("asdf")))
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

