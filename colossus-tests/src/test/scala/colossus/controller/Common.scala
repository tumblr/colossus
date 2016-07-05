package colossus
package controller

import core._
import testkit._
import akka.actor._
import scala.concurrent.duration._

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

trait TestController[E <: Encoding] extends ControllerIface[E] { self: StaticController[E] with ServerConnectionHandler =>

  implicit val namespace = {
    import metrics._
    MetricContext("/", Collection.withReferenceConf(Seq()))
  }

  var _received : Seq[E#Input] = Seq()
  def received = _received

  def receivedMessage(message: Any,sender: akka.actor.ActorRef): Unit = {}

  def processMessage(message: E#Input) {
    _received = received :+ message
  }


  def testPause() { pauseWrites() }
  def testResume() { resumeWrites() }

  //these methods just expose protected versions
  def testPush(message: E#Output)(onPush: OutputResult => Unit) {
    push(message)(onPush)
  }

  def pPush(message: E#Output): PushPromise = {
    val p = new PushPromise
    p.pushed = push(message)(p.func)
    p
  }
}

object TestController {
  import RawProtocol._

  val defaultConfig = ControllerConfig(4, 50.milliseconds)

  type T[E <: Encoding] = StaticController[E] with TestController[E] with ServerConnectionHandler

  def controller[E <: Encoding](codec: Codec[E], config: ControllerConfig)(implicit sys: ActorSystem): TypedMockConnection[T[E]] = {
    val con =MockConnection.server(
      c => {
        val t: T[E] = new BasicController[E](codec, config, c.context) with StaticController[E] with TestController[E] with ServerConnectionHandler
        t
      },
      500
    )
    con.handler.connected(con)
    con
  }

  def static(config: ControllerConfig = defaultConfig)(implicit sys: ActorSystem) = controller(RawServerCodec, config)

}
