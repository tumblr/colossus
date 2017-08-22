package colossus.controller

import akka.util.ByteString
import colossus.core.{DisconnectCause, DynamicOutBuffer}
import colossus.streaming.{PushResult, Source}
import colossus.testkit.ColossusSpec

class OutputControllerSpec extends ColossusSpec with ControllerMocks {

  "OutputController" must {
    "push a message" in {
      val (u, con, d) = get()
      val message     = ByteString("Hello World!")
      con.connected()
      con.outgoing.push(message)
      expectWrite(con, message)
    }
    "push multiple messages" in {
      val (u, con, d) = get()
      val data        = ByteString("Hello World!")
      con.connected()
      con.outgoing.push(data)
      con.outgoing.push(data)
      expectWrite(con, data ++ data)
    }

    "respect buffer soft overflow" in {
      val (u, con, d) = get()
      val over        = ByteString("abc")
      val next        = ByteString("hey")
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

    //IT IS NOW IMPOSSIBLE TO TEST THESE, since closed/terminated no longer leak
    //outside of circuit-breakers, there is no way to cause this to happen

    "react to terminated output buffer" ignore {
      //the buffer has to be terminated in the middle of the call to
      //readyForData, since otherwise the CircuitBreaker will hide it
      val (u, con, d) = get()
      con.connected()
      val upstream = Source
        .fromArray((0 to 50).toArray)
        .map { i =>
          if (i > 10) { con.outgoing.terminate(new Exception("ASDF")) }; ByteString(i.toString)
        }
      upstream into con.outgoing
      con.readyForData(new DynamicOutBuffer(1000))
      (con.upstream.kill _).verify(*)
    }

    "react to closed output buffer" ignore {
      val (u, con, d) = get()
      con.connected()
      //this pipe will close when the iterator is finished
      val upstream = Source.fromIterator((0 to 5).map { i =>
        ByteString(i.toString)
      }.toIterator)
      upstream into con.outgoing
      con.readyForData(new DynamicOutBuffer(1000))
      (con.upstream.kill _).verify(*)
    }

  }

}
