package colossus
package controller

import akka.util.ByteString
import colossus.core._
import colossus.parsing.DataSize._
import colossus.testkit._
import colossus.streaming._
import org.scalamock.scalatest.MockFactory

import scala.util.Success
import SimpleProtocol._

class InputControllerSpec extends ColossusSpec with CallbackMatchers with ControllerMocks {
  
  "Input Controller" must {

    "receive a message" in {
      val input = ByteString("5;hello6;world!")
      val (u, con, d) = get(new SimpleCodec, defaultConfig)
      con.receivedData(DataBuffer(input))
      d.pipe.pull mustBe PullResult.Item(ByteString("hello"))
      d.pipe.pull mustBe PullResult.Item(ByteString("world!"))
    }

    
    "reject data above the size limit" in {
      val input = ByteString("5;hello")
      val config = ControllerConfig(4, 2.bytes)
      val (u, con, d) = get(config)
      con.receivedData(DataBuffer(input))
      (u.disconnect _).verify()
    }

    "properly copy and buffer data when input buffer fills" in {
      //input buffer size set to 3 (see common.scala) so it will have to take a copy
      val input = DataBuffer(ByteString("1;a;1;b;1;c;1;d;1;e;1;f"))
      val (u, con, d) = get(new SimpleCodec, defaultConfig)
      con.receivedData(input)
      input.remaining mustBe 0
      (1 to 6).foreach {_ => 
        d.pipe.pull() mustBe a[PullResult.Item[_]]
      }
    }

  }

}

