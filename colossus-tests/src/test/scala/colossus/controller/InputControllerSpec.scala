package colossus
package controller

import akka.util.ByteString
import colossus.core._
import colossus.parsing.DataSize._
import colossus.testkit._

import scala.concurrent.duration._
import scala.util.Success

class InputControllerSpec extends ColossusSpec with CallbackMatchers{
  
  import TestController._

  "Input Controller" must {

    "receive a message" in {
      val input = ByteString("hello")
      val con = static()
      con.typedHandler.receivedData(DataBuffer(input))
      con.typedHandler.received.size must equal(1)
      con.typedHandler.received.head must equal(input)
    }

    
    "reject data above the size limit" in {
      val input = ByteString("hello")
      val config = ControllerConfig(4, 50.milliseconds, 2.bytes)
      val con = static(config)
      con.typedHandler.receivedData(DataBuffer(input))
      con.typedHandler.received.isEmpty must equal(true)
      con.typedHandler.connectionState mustBe a[ConnectionState.ShuttingDown]
    }

  }

}

