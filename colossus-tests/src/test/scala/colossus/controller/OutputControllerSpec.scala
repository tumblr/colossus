package colossus
package controller

import scala.util.{Try, Success, Failure}
import core._
import testkit._
import service.Codec
import org.scalatest._
import akka.util.ByteString



class OutputControllerSpec extends WordSpec with MustMatchers {

  
  class TestController extends OutputController[TestInput, TestOutput] {
    def codec = TestCodec

    def connected(endpoint: colossus.core.WriteEndpoint): Unit = ???
    protected def connectionClosed(cause: colossus.core.DisconnectCause): Unit = ???
    protected def connectionLost(cause: colossus.core.DisconnectError): Unit = ???
    def idleCheck(period: scala.concurrent.duration.Duration): Unit = ???
    def readyForData(): Unit = ???

    def controllerConfig: colossus.controller.ControllerConfig = ControllerConfig(4)
    def receivedMessage(message: Any,sender: akka.actor.ActorRef): Unit = ???

    def testPush(message: TestOutput) {
      push(message)
    }
  }

  def createController: (MockWriteEndpoint, TestController) = {
    val controller = new TestController
    val (probe, worker) = FakeIOSystem.fakeWorkerRef
    controller.setBind(1, worker)
    val endpoint = new MockWriteEndpoint(100, probe, Some(controller))
    controller.connected(endpoint)
    (endpoint, controller)
  }

  "OutputController" must {
    "push a message" in {
      val (endpoint, controller) = createController
      val data = ByteString("Hello World!")
      val message = TestOutput(Sink.one(DataBuffer(data)))
      controller.testPush(message)
      endpoint.writeCalls(0) must equal(data)

    }
  }



}
