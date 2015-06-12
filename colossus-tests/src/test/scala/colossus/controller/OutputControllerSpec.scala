package colossus
package controller

import core._
import testkit._
import akka.util.ByteString



class OutputControllerSpec extends ColossusSpec {

 import TestController.createController 

  "OutputController" must {
    "push a message" in {
      val (endpoint, controller) = createController()
      val data = ByteString("Hello World!")
      val message = TestOutput(Source.one(DataBuffer(data)))
      controller.testPush(message){_ must equal (OutputResult.Success)}
      endpoint.expectOneWrite(data)

    }
    "push multiple messages" in {
      val (endpoint, controller) = createController()
      val data = ByteString("Hello World!")
      val message = TestOutput(Source.one(DataBuffer(data)))
      controller.testPush(message){_ must equal (OutputResult.Success)}
      endpoint.expectOneWrite(data)

      val message2 = TestOutput(Source.one(DataBuffer(data)))
      controller.testPush(message2){_ must equal (OutputResult.Success)}
      endpoint.expectOneWrite(data)

    }

    "drain output buffer on graceful disconnect" in {
      val (endpoint, controller) = createController()
      val data = ByteString(List.fill(endpoint.maxWriteSize + 1)("x").mkString)
      val message = TestOutput(Source.one(DataBuffer(data)))
      val data2 = ByteString("m2")
      val message2 = TestOutput(Source.one(DataBuffer(data2)))

      controller.testPush(message){_ must equal (OutputResult.Success)}
      controller.testPush(message2){_ must equal (OutputResult.Success)}
      controller.testGracefulDisconnect()
      endpoint.expectOneWrite(data.take(endpoint.maxWriteSize))
      endpoint.disconnectCalled must equal(false)
      endpoint.clearBuffer()
      endpoint.expectWrite(data.drop(endpoint.maxWriteSize))
      endpoint.expectWrite(data2)
      endpoint.disconnectCalled must equal(true)  
    }

    "timeout queued messages that haven't been sent" in {
      val (endpoint, controller) = createController()
      val data = ByteString(List.fill(endpoint.maxWriteSize + 1)("x").mkString)

      val message = TestOutput(Source.one(DataBuffer(data)))
      val message2 = TestOutput(Source.one(DataBuffer(data)))

      controller.testPush(message){_ must equal (OutputResult.Success)}
      controller.testPush(message2){_ must equal (OutputResult.Cancelled)}

    }
  }



}
