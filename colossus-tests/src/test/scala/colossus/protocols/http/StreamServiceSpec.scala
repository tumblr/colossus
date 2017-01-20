package colossus
package protocols.http
package streaming

import core._
import controller._
import testkit._
import colossus.streaming._
import GenEncoding._
import akka.util.ByteString

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

  type Con = HttpStreamServerController

  def create(): (Con, TestUpstream[Encoding.Server[StreamHttp]])  = {
    val controllerstub = new TestUpstream[Encoding.Server[StreamHttp]]
    val handler = new HttpStreamServerController(stub[ControllerDownstream[Encoding.Server[StreamingHttp]]])
    handler.setUpstream(controllerstub)
    (handler, controllerstub)
  }


  "StreamServiceController" must {
    "flatten and push a response" in {
      val (ctrlr, stub) = create()
      val response = HttpResponse.ok("helllo")
      val s = StreamingHttpResponse(response)
      ctrlr.connected()
      //you can thank Scala 2.10 for this insanity
      ctrlr.outgoing.push(s)
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

    def expectDisconnect(f: Con => Any) {
      val (ctrlr, stub) = create()
      val p = new BufferedPipe[StreamingHttpMessage[HttpRequestHead]](1)
      (ctrlr.downstream.incoming _).when().returns(p)
      ctrlr.connected()
      f(ctrlr)
      (stub.connection.forceDisconnect _).verify()
    }

    "disconnect if head received before end" in expectDisconnect { ctrlr => 
      ctrlr.incoming.push(Head(HttpRequest.get("/foo").head))
      ctrlr.incoming.push(Head(HttpRequest.get("/foo").head))
    }

    "disconnect if end received before head" in expectDisconnect { ctrlr => 
      ctrlr.incoming.push(End)
    }

    "disconnect if Data received before head" in expectDisconnect { ctrlr =>
      ctrlr.incoming.push(Data(DataBlock("WAT")))
    }

    "disconnect if pipe is terminated" in expectDisconnect { ctrlr =>
      ctrlr.incoming.terminate(new Exception("WHATAT"))
    }

    "disconnect if downstream pipe closes" in expectDisconnect { ctrlr =>
      ctrlr.downstream.incoming.complete()
      ctrlr.incoming.push(Head(HttpRequest.get("/foo").head))
    }
      

  }

  "StreamingHttpClient" must {
    "correctly send a request" taggedAs(org.scalatest.Tag("test")) in {
      val (con, client) = MockConnection.apiClient(implicit w => StreamingHttpClient.client("localhost", TEST_PORT))
      con.handler.connected(con)
      val req = StreamingHttpRequest(
        HttpRequestHead(HttpMethod.Get, "/foo", HttpVersion.`1.1`, HttpHeaders.Empty), 
        Source.fromArray(Array("foo", "bar", "baz")).map{s => Data(DataBlock(s))}
      )
      client.send(req).execute()
      val expected = "GET /foo HTTP/1.1\r\nhost: localhost\r\n\r\nfoobarbaz"
      con.iterate()
      con.expectOneWrite(ByteString(expected))
    }

    "correctly receive a response" in {
      val (con, client) = MockConnection.apiClient(implicit w => StreamingHttpClient.client("localhost", TEST_PORT))
      con.handler.connected(con)
      val req = StreamingHttpRequest(
        HttpRequestHead(HttpMethod.Get, "/foo", HttpVersion.`1.1`, HttpHeaders.Empty), 
        Source.fromArray(Array("foo", "bar", "baz")).map{s => Data(DataBlock(s))}
      )
      var resp: Option[StreamingHttpResponse] = None
      client.send(req).map{r => resp = Some(r)}.execute()
      con.iterate()
      resp mustBe None
      con.handler.receivedData(DataBuffer("HTTP/1.1 200 OK\r\nTransfer-encoding: chunked\r\n"))
      resp mustBe None
      con.handler.receivedData(DataBuffer("\r\n2\r\nhi\r\n"))
      resp mustBe a[Some[StreamingHttpResponse]]
      val r = resp.get
      r.body.pull().asInstanceOf[PullResult.Item[Data]].item.data.utf8String mustBe "hi"
      r.body.pull() mustBe a[PullResult.Empty]
      con.handler.receivedData(DataBuffer("0\r\n\r\n"))
      r.body.pull() mustBe PullResult.Closed
    }
  }

}
