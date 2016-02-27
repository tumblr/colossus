package colossus
package controller

import core._
import service._
import testkit._
import akka.util.ByteString

import scala.concurrent.duration._

import RawProtocol._


class OutputControllerSpec extends ColossusSpec {

  import TestController.createController 
  val config = ControllerConfig(4, 50.milliseconds)

  type T[I,O] = Controller[I,O] with TestController[I,O] with ServerConnectionHandler

  def controller[I,O](codec: Codec[O,I]): TypedMockConnection[T[I,O]] = {
    val con =MockConnection.server(
      c => new Controller[I,O](codec, config, c.context) with TestController[I, O] with ServerConnectionHandler, 
      500
    )
    con.handler.connected(con)
    con
  }

  def static() = controller(RawCodec)
  def stream() = controller(new TestCodec)

  "OutputController" must {
    "push a message" in {
      val endpoint = static()
      val message = ByteString("Hello World!")
      val p = endpoint.typedHandler.pPush(message)
      p.isSet must equal(false)
      endpoint.iterate()
      endpoint.expectOneWrite(message)
      p.isSuccess must equal(true)

    }
    "push multiple messages" in {
      val endpoint = static()
      val data = ByteString("Hello World!")
      val p1 = endpoint.typedHandler.pPush(data)
      val p2 = endpoint.typedHandler.pPush(data)
      endpoint.iterate()
      endpoint.expectOneWrite(data ++ data)
      p1.expectSuccess()
      p2.expectSuccess()

    }

    "respect buffer soft overflow" in {
      val endpoint = static()
      val over = ByteString(List.fill(110)("a").mkString)
      val next = ByteString("hey")
      val p1 = endpoint.typedHandler.pPush(over)
      val p2 = endpoint.typedHandler.pPush(next)
      endpoint.iterate()
      p1.expectSuccess()
      p2.expectNoSet
      endpoint.iterate()
      p2.expectSuccess()
    }
      

    "drain output buffer on disconnect" in {
      val endpoint = static()
      val over = ByteString(List.fill(110)("a").mkString)
      val next = ByteString("hey")
      val p1 = endpoint.typedHandler.pPush(over)
      val p2 = endpoint.typedHandler.pPush(next)
      endpoint.typedHandler.disconnect()
      endpoint.workerProbe.expectNoMsg(100.milliseconds)
      endpoint.iterate()
      p1.expectSuccess()
      p2.expectNoSet
      endpoint.iterate()
      p2.expectSuccess()
      //final iterate is needed to do the disconnect check
      endpoint.iterate()
      endpoint.workerProbe.expectMsg(100.milliseconds, WorkerCommand.Disconnect(endpoint.id))
    }

    "timeout queued messages that haven't been sent" in {
      val endpoint = static()
      val p = endpoint.typedHandler.pPush(ByteString("wat"))
      Thread.sleep(50)
      endpoint.handler.idleCheck(1.millisecond)
      println(p.result)
      p.expectCancelled()
    }


    "fail pending messages on connectionClosed while gracefully disconnecting"  in {
      val (endpoint, controller) = createController(outputBufferSize = 10)
      val data = ByteString(List.fill(endpoint.maxWriteSize * 2)("x").mkString)

      val message = TestOutput(Source.one(DataBuffer(data)))
      val message2 = TestOutput(Source.one(DataBuffer(data)))

      val p1 = controller.pPush(message)
      val p2 = controller.pPush(message2)
      p1.pushed must equal(true)
      p2.pushed must equal(true)
      controller.disconnect()
      endpoint.disconnectCalled must equal(false)
      endpoint.disrupt()
      p1.result.get.isInstanceOf[OutputResult.Failure] must equal(true)
      p2.result.get.isInstanceOf[OutputResult.Cancelled] must equal(true)

      
    }

    "not fail pending messages when graceful disconnect after connection disrupted" taggedAs(org.scalatest.Tag("test")) in {
      //notice that disrupting a connection does not flush the pending buffer.
      //The behavior used to be that calling gracefulDisconnect after a
      //disruption would trigger a flush of the buffer, but since now the
      //shutdown procedure does not occur if the connection is not connected,
      //this doesn't happen anymore
      val (endpoint, controller) = createController(outputBufferSize = 10)
      val data = ByteString(List.fill(endpoint.maxWriteSize * 2)("x").mkString)

      val message = TestOutput(Source.one(DataBuffer(data)))
      val message2 = TestOutput(Source.one(DataBuffer(data)))

      val p1 = controller.pPush(message)
      val p2 = controller.pPush(message2)
      endpoint.disrupt()
      p1.result.get.isInstanceOf[OutputResult.Failure] must equal(true)
      p2.isSet must equal(false)
      controller.disconnect()
      p2.isSet must equal(false)

    }


      
  }



}
