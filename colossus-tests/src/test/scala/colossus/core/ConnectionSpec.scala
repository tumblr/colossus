package colossus
package core

import testkit._
import java.nio.channels.SocketChannel
import org.scalatest.mock.MockitoSugar

import akka.testkit.TestProbe
import akka.util.ByteString

import scala.concurrent.duration._

class ConnectionSpec extends ColossusSpec with MockitoSugar{

  "Connection" must {


    "catch exceptions thrown in handler's connectionTerminated when connection closed" in {
      val con = MockConnection.client(new BasicSyncHandler(_) with ClientConnectionHandler {

        override def connectionClosed(cause: DisconnectCause) {
          println("here")
          throw new Exception("x_x")
        }

        override def connectionLost(error: DisconnectError) {
          throw new Exception("o_O")
        }

        def receivedData(data: DataBuffer){}
      })

      //this test fails if this throws an exception
      con.close(DisconnectCause.Closed)
      con.close(DisconnectCause.Disconnect)
    }

  }


  "ClientConnection" must {
    "timeout idle connection" in {
      val con = MockConnection.client(new BasicSyncHandler(_) with ClientConnectionHandler {
        override def maxIdleTime = 100.milliseconds
        def receivedData(data:DataBuffer){}
      })
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

