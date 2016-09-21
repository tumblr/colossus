package colossus
package controller

import core._
import testkit._
import akka.actor._
import akka.util.ByteString
import scala.concurrent.duration._
import org.scalamock.scalatest.MockFactory
import colossus.parsing.DataSize._

import RawProtocol._

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

trait TestController[T <: Encoding] extends Controller[T] {
  
  def pPush(message: T#Output): PushPromise = {
    val p = new PushPromise
    push(message)(p.func)
    p
  }
}

trait ControllerMocks extends MockFactory {self: org.scalamock.scalatest.MockFactory with org.scalatest.Suite =>

  val defaultConfig = ControllerConfig(4, 50.milliseconds, 2000.bytes)

  def get(config: ControllerConfig = defaultConfig)(implicit sys: ActorSystem): (CoreUpstream, TestController[Raw#ServerEncoding], ControllerDownstream[Raw#ServerEncoding]) = {
    val upstream = stub[CoreUpstream]
    val downstream = stub[ControllerDownstream[Raw#ServerEncoding]]
    (downstream.controllerConfig _).when().returns(config)
    (downstream.context _).when().returns(FakeIOSystem.fakeContext)
    (downstream.onFatalError _).when(*).returns(None)

    val controller = new Controller(downstream, RawServerCodec) with TestController[Raw#ServerEncoding]
    controller.setUpstream(upstream)
    (upstream, controller, downstream)
  }

  def expectWrite(c: CoreDownstream, expected: ByteString, bufferSize: Int = 100) {
    val d = new DynamicOutBuffer(bufferSize)
    c.readyForData(d)
    assert(ByteString(d.data.takeAll) == expected)
  }
}
