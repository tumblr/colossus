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

case class TestInputImpl(data: FiniteBytePipe) extends TestInput{
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

//simple helper class for testing push results, just stores the value so we can
//check if it gets set at all and to the right value
class PushPromise {

  private var _result: Option[OutputResult] = None
  var pushed = false

  def result = _result

  val func: OutputResult => Unit = r => _result = Some(r)

  def isSet = result.isDefined

}

class TestController(dataBufferSize: Int, processor: TestInput => Unit, context: Context) extends Controller[TestInput, TestOutput](new TestCodec, ControllerConfig(4, dataBufferSize, 50.milliseconds), context) with ServerConnectionHandler {

  def receivedMessage(message: Any,sender: akka.actor.ActorRef): Unit = ???

  def processMessage(message: TestInput) {
    processor(message)
  }


  //these methods just expose protected versions
  def testPush(message: TestOutput)(onPush: OutputResult => Unit) {
    push(message)(onPush)
  }

  def pPush(message: TestOutput): PushPromise = {
    val p = new PushPromise
    p.pushed = push(message)(p.func)
    p
  }


  def testGracefulDisconnect() {
    gracefulDisconnect()
  }
}

object TestController {

  //TODO just return TypedMockConnection instead of the tuple
  def createController(outputBufferSize: Int = 100, dataBufferSize: Int = 100, processor: TestInput => Unit = x => ())(implicit system: ActorSystem): (MockConnection, TestController) = {
    val endpoint = MockConnection.server(context => new TestController(dataBufferSize, processor, context.context), outputBufferSize)
    endpoint.handler.connected(endpoint)
    (endpoint, endpoint.typedHandler)
  }

  def createController(processor: TestInput => Unit)(implicit system: ActorSystem): (MockConnection, TestController) = createController(100, 100, processor)
}
