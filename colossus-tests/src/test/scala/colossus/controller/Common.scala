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
  def isSuccess = result == Some(OutputResult.Success)
  def isCancelled = result.isDefined && result.get.isInstanceOf[OutputResult.Cancelled]
  def isFailure = result.isDefined && result.get.isInstanceOf[OutputResult.Failure]

  def expectNoSet() { assert(isSet == false) }
  def expectSuccess() { assert(isSuccess == true) }
  def expectFailure() {  assert(isFailure == true)}
  def expectCancelled() {  assert(isCancelled == true)}
}

trait TestController[I,O] { self: Controller[I,O] with ServerConnectionHandler =>

  implicit val namespace = {
    import metrics._
    MetricContext("/", Collection.withReferenceConf(Seq()))
  }

  var _received : Seq[I] = Seq()
  def received = _received

  def receivedMessage(message: Any,sender: akka.actor.ActorRef): Unit = {}

  def processMessage(message: I) {
    _received = received :+ message
  }


  def testPause() { pauseWrites() }
  def testResume() { resumeWrites() }

  //these methods just expose protected versions
  def testPush(message: O)(onPush: OutputResult => Unit) {
    push(message)(onPush)
  }

  def pPush(message: O): PushPromise = {
    val p = new PushPromise
    p.pushed = push(message)(p.func)
    p
  }
}

object TestController {
  import RawProtocol.RawCodec

  val defaultConfig = ControllerConfig(4, 50.milliseconds)

  type T[I,O] = Controller[I,O] with TestController[I,O] with ServerConnectionHandler

  def controller[I,O](codec: Codec[O,I], config: ControllerConfig)(implicit sys: ActorSystem): TypedMockConnection[T[I,O]] = {
    val con =MockConnection.server(
      c => new Controller[I,O](codec, config, c.context) with TestController[I, O] with ServerConnectionHandler,
      500
    )
    con.handler.connected(con)
    con
  }

  def static(config: ControllerConfig = defaultConfig)(implicit sys: ActorSystem) = controller(RawCodec, config)
  def stream(config: ControllerConfig = defaultConfig)(implicit sys: ActorSystem) = controller(new TestCodec, config)

}
