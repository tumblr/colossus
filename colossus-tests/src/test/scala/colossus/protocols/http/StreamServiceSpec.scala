package colossus
package protocols.http

import core._
import controller._
import testkit._
import stream._
import streaming._
import GenEncoding._

import org.scalamock.scalatest.MockFactory

class StreamServiceSpec extends ColossusSpec with MockFactory{

  "Streaming Http Response" must {
    "build from regular response" in {
      val r = HttpResponse.ok("Hello")
      val s = StreamingHttpResponse(r)
      val data = s.collapse
      data.pull() mustBe PullResult.Item(Head(r.head))
      data.pull() mustBe PullResult.Item(Data(r.body.asDataBlock))
      data.pull() mustBe PullResult.Item(End)
    }
  }

  type Con = StreamServiceController[Encoding.Server[StreamHeader]]

  def create(): (Con, ControllerUpstream[Encoding.Server[StreamHttp]])  = {
    val controllerstub = stub[ControllerUpstream[Encoding.Server[StreamHttp]]]
    val connectionstub = stub[ConnectionManager]
    (connectionstub.isConnected _).when().returns(true)
    (controllerstub.connection _).when().returns(connectionstub)
    (controllerstub.pushFrom _).when(*, *, *).returns(true)
    (controllerstub.canPush _).when().returns(true)
    
    val handler = new StreamServiceController[Encoding.Server[StreamHeader]](stub[ControllerDownstream[Encoding.Server[StreamingHttp]]], StreamRequestBuilder)
    handler.setUpstream(controllerstub)
    (handler, controllerstub)
  }

  def expectPush(controller: ControllerUpstream[Encoding.Server[StreamHttp]], message: HttpStream[HttpResponseHead]) {
      (controller.pushFrom _ ).verify(message, *, *)
  }

  "StreamServiceController" must {
    "flatten and push a response" in {
      val (ctrlr, stub) = create()
      val response = HttpResponse.ok("helllo")
      ctrlr.connected()
      ctrlr.push(StreamingHttpResponse(response))(_ => ())
      expectPush(stub, Head(response.head))
      expectPush(stub, Data(response.body.asDataBlock))
      expectPush(stub, End)
    }
  }

}
      
