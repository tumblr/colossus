package colossus
package controller

import core._
import testkit._
import akka.util.ByteString

import scala.concurrent.duration._


class OutputControllerSpec extends ColossusSpec {

 import TestController.createController 

  "OutputController" must {
    "push a message" in {
      val (endpoint, controller) = createController()
      val data = ByteString("Hello World!")
      val message = TestOutput(Source.one(DataBuffer(data)))
      endpoint.iterate{
        controller.testPush(message){_ must equal (OutputResult.Success)}
      }
      endpoint.expectOneWrite(data)

    }
    "push multiple messages" in {
      val (endpoint, controller) = createController()
      val data = ByteString("Hello World!")
      val message = TestOutput(Source.one(DataBuffer(data)))
      val message2 = TestOutput(Source.one(DataBuffer(data)))
      endpoint.iterate{
        controller.testPush(message){_ must equal (OutputResult.Success)}
        controller.testPush(message2){_ must equal (OutputResult.Success)}
      }
      endpoint.expectOneWrite(data ++ data)

    }

    "properly react to full data buffer" in {
      val (endpoint, controller) = createController(outputBufferSize = 100, dataBufferSize = 10)
      val data = ByteString("123456")
      endpoint.iterate {
        controller.testPush(TestOutput(Source.one(DataBuffer(data)))){_ must equal (OutputResult.Success)}
        controller.testPush(TestOutput(Source.one(DataBuffer(data)))){_ must equal (OutputResult.Success)}
        controller.testPush(TestOutput(Source.one(DataBuffer(data)))){_ must equal (OutputResult.Success)}
      }
      //data buffer size is set to 10, so two messages should fill it, this means we should expect the three messages to occur as two writes to the WriteBuffer
      endpoint.expectWrite((data ++ data))
      endpoint.expectWrite(data)
      endpoint.writeReadyEnabled must equal(false)
    }

    "properly react to full output buffer"   in {
      val (endpoint, controller) = createController(outputBufferSize = 10, dataBufferSize = 100)
      val data = ByteString("123456")
      endpoint.iterate {
        controller.testPush(TestOutput(Source.one(DataBuffer(data)))){_ must equal (OutputResult.Success)}
        controller.testPush(TestOutput(Source.one(DataBuffer(data)))){_ must equal (OutputResult.Success)}
        controller.testPush(TestOutput(Source.one(DataBuffer(data)))){_ must equal (OutputResult.Success)}
      }
      
      endpoint.expectOneWrite((data ++ data).take(10), true)
      endpoint.writeReadyEnabled must equal(true)
      endpoint.clearBuffer()
      endpoint.iterate({})
      endpoint.expectOneWrite((data ++ data).drop(10) ++ data, true)
      endpoint.writeReadyEnabled must equal(false)

    }



    "drain output buffer on graceful disconnect" in {
      val (endpoint, controller) = createController()
      val data = ByteString(List.fill(endpoint.maxWriteSize + 1)("x").mkString)
      val message = TestOutput(Source.one(DataBuffer(data)))
      val data2 = ByteString("m2")
      val message2 = TestOutput(Source.one(DataBuffer(data2)))
      endpoint.iterate {
        controller.testPush(message){_ must equal (OutputResult.Success)}
        controller.testPush(message2){_ must equal (OutputResult.Success)}
        controller.testGracefulDisconnect()
      }
      endpoint.expectOneWrite(data.take(endpoint.maxWriteSize))
      endpoint.workerProbe.expectNoMsg(100.milliseconds)
      endpoint.clearBuffer()
      endpoint.iterate({})
      //these occur as separate writes because the first comes from the partial buffer, the second from the controller
      endpoint.expectWrite(data.drop(endpoint.maxWriteSize))
      endpoint.expectWrite(data2)
      endpoint.workerProbe.expectMsg(100.milliseconds, WorkerCommand.Disconnect(controller.id.get))
    }

    "timeout queued messages that haven't been sent" in {
      val (endpoint, controller) = createController(10, 10)
      val data = ByteString(List.fill(endpoint.maxWriteSize * 2)("x").mkString)

      val message = TestOutput(Source.one(DataBuffer(data)))
      val message2 = TestOutput(Source.one(DataBuffer(data)))

      val p1 = controller.pPush(message)
      val p2 = controller.pPush(message2)
      Thread.sleep(300)
      controller.idleCheck(1.millisecond)
      p1.result.isDefined must equal(false) //in the process of being sent
      p2.result.get.isInstanceOf[OutputResult.Cancelled] must equal(true)
      

    }

    "allow multiple calls to gracefulDisconnect" in {
      val (endpoint, controller) = createController()
      controller.testGracefulDisconnect()
      //this used to throw an exception
      controller.testGracefulDisconnect()
    }

    "fail pending messages on connectionClosed while gracefully disconnecting"  in {
      val (endpoint, controller) = createController(outputBufferSize = 10, dataBufferSize = 10)
      val data = ByteString(List.fill(endpoint.maxWriteSize * 2)("x").mkString)

      val message = TestOutput(Source.one(DataBuffer(data)))
      val message2 = TestOutput(Source.one(DataBuffer(data)))

      val p1 = controller.pPush(message)
      val p2 = controller.pPush(message2)
      p1.pushed must equal(true)
      p2.pushed must equal(true)
      controller.testGracefulDisconnect()
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
      val (endpoint, controller) = createController(outputBufferSize = 10, dataBufferSize = 10)
      val data = ByteString(List.fill(endpoint.maxWriteSize * 2)("x").mkString)

      val message = TestOutput(Source.one(DataBuffer(data)))
      val message2 = TestOutput(Source.one(DataBuffer(data)))

      val p1 = controller.pPush(message)
      val p2 = controller.pPush(message2)
      endpoint.disrupt()
      p1.result.get.isInstanceOf[OutputResult.Failure] must equal(true)
      p2.isSet must equal(false)
      controller.testGracefulDisconnect()
      p2.isSet must equal(false)

    }


      
  }



}
