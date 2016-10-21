package colossus
package controller

import akka.util.ByteString
import colossus.core._
import colossus.parsing.DataSize._
import colossus.testkit._
import colossus.streaming._
import org.scalamock.scalatest.MockFactory

import scala.concurrent.duration._
import scala.util.Success

class InputControllerSpec extends ColossusSpec with CallbackMatchers with ControllerMocks {
  
  "Input Controller" must {

    "receive a message" in {
      val input = ByteString("hello")
      val (u, con, d) = get()
      con.receivedData(DataBuffer(input))
      d.pipe.pull mustBe PullResult.Item(ByteString("hello"))
    }

    
    "reject data above the size limit" in {
      val input = ByteString("hello")
      val config = ControllerConfig(4, 50.milliseconds, 2.bytes)
      val (u, con, d) = get(config)
      con.receivedData(DataBuffer(input))
      (u.disconnect _).verify()
    }

  }

}

