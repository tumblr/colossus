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

    "properly react to full output buffer" taggedAs(org.scalatest.Tag("test"))  in {
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
      endpoint.disconnectCalled must equal(false)
      endpoint.clearBuffer()
      endpoint.iterate({})
      //these occur as separate writes because the first comes from the partial buffer, the second from the controller
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

    "allow multiple calls to gracefulDisconnect" in {
      val (endpoint, controller) = createController()
      controller.testGracefulDisconnect()
      //this used to throw an exception
      controller.testGracefulDisconnect()
    }


      
  }



}
