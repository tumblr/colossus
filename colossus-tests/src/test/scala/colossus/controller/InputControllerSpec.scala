package colossus
package controller

import scala.util.{Try, Success, Failure}
import core._
import service.Codec
import org.scalatest._
import akka.util.ByteString



class InputControllerSpec extends WordSpec with MustMatchers {
  

  class TestController(processor: TestInput => Unit) extends InputController[TestInput, TestOutput] {
    def codec = new TestCodec

    def processMessage(message: TestInput) {
      processor(message)
    }
    def connected(endpoint: colossus.core.WriteEndpoint): Unit = ???
    protected def connectionClosed(cause: colossus.core.DisconnectCause): Unit = ???
    protected def connectionLost(cause: colossus.core.DisconnectError): Unit = ???
    def idleCheck(period: scala.concurrent.duration.Duration): Unit = ???
    def readyForData(): Unit = ???

    def controllerConfig: colossus.controller.ControllerConfig = ControllerConfig(5)
    def receivedMessage(message: Any,sender: akka.actor.ActorRef): Unit = ???
  }

  "Input Controller" must {
    "decode a stream message" in {
      val expected = ByteString("Hello world!")
      val request = ByteString(expected.size.toString) ++ ByteString("\r\n") ++ expected
      var called = false
      val con = new TestController({input => 
        input.sink.pullCB().execute{
          case Success(Some(data)) => {
            ByteString(data.takeAll) must equal(expected)
            called = true
          }
          case _ => throw new Exception("wrong result")
        }
      })
      called must equal(false)
      con.receivedData(DataBuffer(request))
      called must equal(true)
    }

  }

  


}

