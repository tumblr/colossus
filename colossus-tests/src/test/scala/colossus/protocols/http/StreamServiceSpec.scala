package colossus
package protocols.http

import core._
import controller._
import testkit._
import stream._
import streaming._
import GenEncoding._

import org.scalamock.scalatest.MockFactory
class StreamServiceSpec extends ColossusSpec with MockFactory with ControllerMocks {

  "Streaming Http Response" must {
    "build from regular response" in {
      val r = HttpResponse.ok("Hello")
      val s = StreamingHttpResponse(r)
      val data = s.collapse
      data.pull() mustBe PullResult.Item(Head(r.head))
      data.pull() mustBe PullResult.Item(Data(r.body.asDataBlock))
      data.pull() mustBe PullResult.Item(End)
    }

    "build and collapse a streaming response" in {
      val response = StreamingHttpResponse(
        HttpResponseHead(HttpVersion.`1.1`, HttpCodes.OK,  Some(TransferEncoding.Chunked), None, None, HttpHeaders.Empty), 
        Source.fromIterator(List("hello", "world").toIterator.map{s => Data(DataBlock(s))})
      )
      val collapsed = response.collapse
      collapsed.pull() mustBe PullResult.Item(Head(response.head))
      collapsed.pull() mustBe PullResult.Item(Data(DataBlock("hello")))
      collapsed.pull() mustBe PullResult.Item(Data(DataBlock("world")))
      collapsed.pull() mustBe PullResult.Item(End)
    }
  }

  type Con = StreamServiceController[Encoding.Server[StreamHeader]]

  def create(): (Con, TestUpstream[Encoding.Server[StreamHttp]])  = {
    val controllerstub = new TestUpstream[Encoding.Server[StreamHttp]]
    val handler = new StreamServiceController[Encoding.Server[StreamHeader]](stub[ControllerDownstream[Encoding.Server[StreamingHttp]]], StreamRequestBuilder)
    handler.setUpstream(controllerstub)
    (handler, controllerstub)
  }


  "StreamServiceController" must {
    "flatten and push a response" in {
      val (ctrlr, stub) = create()
      val response = HttpResponse.ok("helllo")
      val s : GenEncoding[StreamingHttpMessage,Encoding.Server[StreamHeader]]#Output = StreamingHttpResponse(response)
      ctrlr.connected()
      //you can thank Scala 2.10 for this insanity
      ctrlr.asInstanceOf[ControllerUpstream[GenEncoding[StreamingHttpMessage,Encoding.Server[StreamHeader]]]].outgoing.push(s.asInstanceOf[GenEncoding[StreamingHttpMessage,Encoding.Server[StreamHeader]]#Output])
      stub.pipe.pull() mustBe PullResult.Item(Head(response.head))
      stub.pipe.pull() mustBe PullResult.Item(Data(response.body.asDataBlock))
      stub.pipe.pull() mustBe PullResult.Item(End)
    }

    "push a chunked response" taggedAs(org.scalatest.Tag("test")) in {
      val (ctrlr, stub) = create()
      val response = StreamingHttpResponse(
        HttpResponseHead(HttpVersion.`1.1`, HttpCodes.OK,  Some(TransferEncoding.Chunked), None, None, HttpHeaders.Empty), 
        Source.fromIterator(List("hello", "world").toIterator.map{s => Data(DataBlock(s))})
      )
      ctrlr.connected()
      ctrlr.outgoing.push(response) mustBe PushResult.Ok
      stub.pipe.pull() mustBe PullResult.Item(Head(response.head))
      stub.pipe.pull() mustBe PullResult.Item(Data(DataBlock("hello")))
      stub.pipe.pull() mustBe PullResult.Item(Data(DataBlock("world")))
      stub.pipe.pull() mustBe PullResult.Item(End)
    }

    "correctly assemble an http request" in {
      val (ctrlr, stub) = create()
      val p = new BufferedPipe[StreamingHttpMessage[HttpRequestHead]](1)
      (ctrlr.downstream.incoming _).when().returns(p)
      val expected = HttpRequest.get("/foo").withHeader("key", "value").withBody(HttpBody("hello there"))
      ctrlr.connected()
      ctrlr.incoming.push(Head(expected.head)) mustBe PushResult.Ok
      ctrlr.incoming.push(Data(expected.body.asDataBlock)) mustBe PushResult.Ok
      ctrlr.incoming.push(End) mustBe PushResult.Ok

      p.pull() match {
        case PullResult.Item(streaming) => {
          streaming.head mustBe expected.head
          streaming.body.pull() mustBe PullResult.Item(Data(DataBlock("hello there")))
          streaming.body.pull() mustBe PullResult.Closed
        }
        case other => throw new Exception("WRONG")
      }
    }

  }

}
