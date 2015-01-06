package colossus
package controller

import scala.util.{Try, Success, Failure}
import core._
import testkit._
import service.Codec
import org.scalatest._
import akka.util.ByteString



class OutputControllerSpec extends ColossusSpec {

 import TestController.createController 

  "OutputController" must {
    "push a message" in {
      val (endpoint, controller) = createController()
      val data = ByteString("Hello World!")
      val message = TestOutput(Source.one(DataBuffer(data)))
      controller.testPush(message){_ must equal (OutputResult.Success)}
      endpoint.writeCalls(0) must equal(data)

    }
  }



}
