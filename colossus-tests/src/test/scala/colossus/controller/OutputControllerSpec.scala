package colossus
package controller

import core._
import streaming._
import testkit._
import akka.util.ByteString

class OutputControllerSpec extends ColossusSpec with ControllerMocks {


  "OutputController" must {
    "push a message" in {
      val (u, con, d) = get()
      val message = ByteString("Hello World!")
      con.connected()
      con.outgoing.push(message)
      expectWrite(con, message)
    }
    "push multiple messages" in {
      val (u, con, d) = get()
      val data = ByteString("Hello World!")
      con.connected()
      con.outgoing.push(data)
      con.outgoing.push(data)
      expectWrite(con, data ++ data)
    }

    "respect buffer soft overflow" in {
      val (u, con, d) = get()
      val over = ByteString("abc")
      val next = ByteString("hey")
      con.connected()
      con.outgoing.push(over)
      con.outgoing.push(next)
      expectWrite(con, ByteString("abc"), 2)
      expectWrite(con, ByteString("hey"), 2)
    }

    "don't allow messages when not connected" in {
      val (u, con, d) = get()
      con.outgoing.push(ByteString("asdf")) mustBe a[PushResult.Full]
      con.connected()
      con.outgoing.push(ByteString("asdf")) mustBe PushResult.Ok
      con.connectionTerminated(DisconnectCause.Disconnect)
      con.outgoing.push(ByteString("asdf")) mustBe a[PushResult.Full]

    }

    "react to terminated output buffer" taggedAs(org.scalatest.Tag("test")) in {
      //the buffer has to be terminated in the middle of the call to
      //readyForData, since otherwise the CircuitBreaker will hide it
      val (u, con, d) = get()
      con.connected()
      var killnow = false
      val upstream = Source
        .fromArray((0 to 50).map{i => ByteString(i.toString)}.toArray)
        .map{i => if (killnow) { con.outgoing.terminate(new Exception("ASDF")) }; i }
      upstream into con.outgoing
      con.readyForData(new DynamicOutBuffer(1000))
      (con.upstream.kill _).verify(*)
    }
      

  }
    



}
