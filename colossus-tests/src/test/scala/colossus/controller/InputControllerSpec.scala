package colossus
package controller

import scala.util.{Try, Success, Failure}
import core._
import service.Codec
import org.scalatest._
import akka.util.ByteString



class InputControllerSpec extends WordSpec with MustMatchers {
  
  trait TestInput {
    def sink: Sink[DataBuffer]
  }
  
  case class TestInputImpl(data: FiniteBytePipe) extends StreamMessage with TestInput{
    def source = data
    def sink = data
  }
    
  case class TestOutput(data: Sink[DataBuffer])
  object TestCodec extends Codec[TestOutput, TestInput]{    
    import parsing.Combinators._
    val parser = intUntil('\r') <~ byte >> {num => TestInputImpl(new FiniteBytePipe(num, 10))}

    def decode(data: DataBuffer): Option[TestInput] = parser.parse(data)

    //TODO: need to add support for pipe combinators, eg A ++ B
    def encode(out: TestOutput) = DataStream(out.data)

    def reset(){}
  }

  class TestController(processor: TestInput => Unit) extends InputController[TestInput, TestOutput] {
    def codec = TestCodec

    def processMessage(message: TestInput) {
      processor(message)
    }
    def connected(endpoint: colossus.core.WriteEndpoint): Unit = ???
    protected def connectionClosed(cause: colossus.core.DisconnectCause): Unit = ???
    protected def connectionLost(cause: colossus.core.DisconnectError): Unit = ???
    def idleCheck(period: scala.concurrent.duration.Duration): Unit = ???
    def readyForData(): Unit = ???

    def controllerConfig: colossus.controller.ControllerConfig = ???
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

