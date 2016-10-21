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
      con.outgoing.push(message)
      expectWrite(con, message)
    }
    "push multiple messages" in {
      val (u, con, d) = get()
      val data = ByteString("Hello World!")
      con.outgoing.push(data)
      con.outgoing.push(data)
      expectWrite(con, data ++ data)
    }

    "respect buffer soft overflow" in {
      val (u, con, d) = get()
      val over = ByteString("abc")
      val next = ByteString("hey")
      con.outgoing.push(over)
      con.outgoing.push(next)
      expectWrite(con, ByteString("abc"), 2)
      expectWrite(con, ByteString("hey"), 2)
    }

    /*

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
    */

      
  }
    



}
