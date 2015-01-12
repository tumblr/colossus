package colossus
package controller

import core._
import colossus.service.{DecodedResult, Codec}
import testkit._
import akka.actor._

trait TestInput {
  def source: Source[DataBuffer]
}

case class TestInputImpl(data: FiniteBytePipe) extends StreamMessage with TestInput{
  def source = data
  def sink = data
}
  
case class TestOutput(data: Source[DataBuffer])


class TestCodec(pipeSize: Int = 3) extends Codec[TestOutput, TestInput]{    
  import parsing.Combinators._
  val parser: Parser[TestInputImpl] = intUntil('\r') <~ byte >> {num => TestInputImpl(new FiniteBytePipe(num, pipeSize))}

  def decode(data: DataBuffer): Option[DecodedResult[TestInput]] = {
    val res = parser.parse(data)
    res.map { x =>
      DecodedResult.Streamed(x, x.source)
    }
  }

  //TODO: need to add support for pipe combinators, eg A ++ B
  def encode(out: TestOutput) = DataStream(out.data)

  def reset(){}
}

class TestController(processor: TestInput => Unit) extends Controller[TestInput, TestOutput](new TestCodec, ControllerConfig(4)) {

  def idleCheck(period: scala.concurrent.duration.Duration): Unit = ???

  def receivedMessage(message: Any,sender: akka.actor.ActorRef): Unit = ???

  def testPush(message: TestOutput)(onPush: OutputResult => Unit) {
    push(message)(onPush)
  }
  def processMessage(message: TestInput) {
    processor(message)
  }
}

object TestController {
  def createController(processor: TestInput => Unit = x => ())(implicit system: ActorSystem): (MockWriteEndpoint, TestController) = {
    val controller = new TestController(processor)
    val (probe, worker) = FakeIOSystem.fakeWorkerRef
    controller.setBind(1, worker)
    val endpoint = new MockWriteEndpoint(100, probe, Some(controller))
    controller.connected(endpoint)
    (endpoint, controller)
  }
}
