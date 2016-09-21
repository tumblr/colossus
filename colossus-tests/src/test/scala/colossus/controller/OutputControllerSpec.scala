package colossus
package controller

import core._
import testkit._
import akka.util.ByteString

import scala.concurrent.duration._



class OutputControllerSpec extends ColossusSpec with ControllerMocks {


  "OutputController" must {
    "push a message" in {
      val (u, con, d) = get()
      val message = ByteString("Hello World!")
      val p = con.pPush(message)
      p.isSet must equal(false)
      expectWrite(con, message)
      p.isSuccess must equal(true)

    }
    "push multiple messages" in {
      val (u, con, d) = get()
      val data = ByteString("Hello World!")
      val p1 = con.pPush(data)
      val p2 = con.pPush(data)
      expectWrite(con, data ++ data)
      p1.expectSuccess()
      p2.expectSuccess()

    }

    "respect buffer soft overflow" in {
      val (u, con, d) = get()
      val over = ByteString("abc")
      val next = ByteString("hey")
      val p1 = con.pPush(over)
      val p2 = con.pPush(next)
      expectWrite(con, ByteString("abc"), 2)
      p1.expectSuccess()
      p2.expectNoSet
      expectWrite(con, ByteString("hey"), 2)
      p2.expectSuccess()
    }

    "respect pausing writes while writing" in {
      val (u, con, d) = get()
      val data = ByteString("hello")
      con.push(data){ case _ => con.pauseWrites() }
      val p = con.pPush(data)
      expectWrite(con, data)
      p.expectNoSet()
      con.resumeWrites()
      expectWrite(con, data)
      p.expectSuccess()
    }

    "not request a write when writes are paused" in {
      val (u, con, d) = get()
      val data = ByteString("hello")
      con.pauseWrites()
      val p = con.pPush(data)
      (u.requestWrite _).verify().never()
    }

    "don't call upstream shutdown on shutdown when there are messages to be sent" in {
      val (u, con, d) = get()
      con.pPush(ByteString("message"))
      con.shutdown()
      (u.shutdown _).verify().never
    }

    "drain output buffer on disconnect" in {
      val (u, con, d) = get()
      val over = ByteString(List.fill(110)("a").mkString)
      val next = ByteString("hey")
      val p1 = con.pPush(over)
      val p2 = con.pPush(next)
      con.shutdown()
      //(u.shutdown _).verify().never
      con.readyForData(new DynamicOutBuffer(100))
      p1.expectSuccess()
      p2.expectNoSet
      //(u.shutdown _).verify().never

      con.readyForData(new DynamicOutBuffer(100))
      p2.expectSuccess()
      //(u.shutdown _).verify().never
      //final iterate is needed to do the disconnect check
      con.readyForData(new DynamicOutBuffer(100))
      (u.shutdown _).verify()
    }

    "timeout queued messages that haven't been sent" in {
      val (u, con, d) = get()
      val p = con.pPush(ByteString("wat"))
      Thread.sleep(300)
      con.idleCheck(1.millisecond)
      p.expectCancelled()
    }


    "fail pending messages on connectionClosed while gracefully disconnecting"  in {
      val (u, con, d) = get()
      val p = con.pPush(ByteString("hello"))
      con.shutdown()
      p.expectNoSet()
      con.connectionTerminated(DisconnectCause.Closed)
      p.expectCancelled()
    }

    "not fail pending messages when connection disrupted" in {
      val (u, con, d) = get()
      val p = con.pPush(ByteString("hello"))
      con.connectionTerminated(DisconnectCause.Closed)
      p.expectNoSet()
    }



      
  }
    



}
