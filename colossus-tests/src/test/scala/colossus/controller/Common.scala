package colossus
package controller

import core._
import colossus.service.{DecodedResult, Codec}
import testkit._
import akka.actor._
import scala.concurrent.duration._

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
  val parser: Parser[TestInputImpl] = intUntil('\r') <~ byte >> {num => TestInputImpl(new FiniteBytePipe(num))}

  def decode(data: DataBuffer): Option[DecodedResult[TestInput]] = {
    val res = parser.parse(data)
    res.map { x =>
      DecodedResult.Stream(x, x.source)
    }
  }

  //TODO: need to add support for pipe combinators, eg A ++ B
  def encode(out: TestOutput) = DataStream(out.data)

  def reset(){}
}

class TestController(dataBufferSize: Int, processor: TestInput => Unit) extends Controller[TestInput, TestOutput](new TestCodec, ControllerConfig(4, dataBufferSize, 1.second)) {

  def receivedMessage(message: Any,sender: akka.actor.ActorRef): Unit = ???

  def processMessage(message: TestInput) {
    processor(message)
  }


  //these methods just expose protected versions
  def testPush(message: TestOutput)(onPush: OutputResult => Unit) {
    push(message)(onPush)
  }
  def testGracefulDisconnect() {
    gracefulDisconnect()
  }
}

object TestController {
  def createController(outputBufferSize: Int = 100, dataBufferSize: Int = 100, processor: TestInput => Unit = x => ())(implicit system: ActorSystem): (MockWriteEndpoint, TestController) = {
    val controller = new TestController(dataBufferSize, processor)
    val (probe, worker) = FakeIOSystem.fakeWorkerRef
    controller.setBind(1, worker)
    val endpoint = new MockWriteEndpoint(outputBufferSize, probe, Some(controller))
    controller.connected(endpoint)
    (endpoint, controller)
  }

  def createController(processor: TestInput => Unit)(implicit system: ActorSystem): (MockWriteEndpoint, TestController) = createController(100, 100, processor)
}
