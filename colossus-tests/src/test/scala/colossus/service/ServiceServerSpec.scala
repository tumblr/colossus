package colossus.service

import akka.util.ByteString
import colossus.RawProtocol._
import colossus.core._
import colossus.controller._
import colossus.parsing.DataSize._
import colossus.testkit._
import colossus.streaming._

import scala.concurrent.duration._
import org.scalamock.scalatest.MockFactory

class ServiceServerSpec extends ColossusSpec with MockFactory with ControllerMocks {
  val config = ServiceConfig.Default.copy(
    requestBufferSize = 2,
    requestTimeout = 50.milliseconds,
    maxRequestSize = 300.bytes
  )

  class FakeHandler(handler: ByteString => Callback[ByteString] = x => Callback.successful(x), context: ServerContext)
      extends GenRequestHandler[Raw](context, config) {

    def handle         = { case req => handler(req) }
    def unhandledError = { case _   => ByteString("ERROR") }
  }

  def fakeService(handler: ByteString => Callback[ByteString] = x => Callback.successful(x))
    : TypedMockConnection[PipelineHandler] = {
    val endpoint = MockConnection.server(ctx => {
      val fh = new FakeHandler(handler, ctx)
      new PipelineHandler(new Controller(new ServiceServer[Raw](fh), RawServerCodec), fh)
    })

    endpoint.handler.connected(endpoint)
    endpoint
  }

  def fs(fn: ByteString => Callback[ByteString] = x => Callback.successful(x))
    : (ServiceServer[Raw], TestUpstream[Encoding.Server[Raw]]) = {
    val upstream = new TestUpstream[Encoding.Server[Raw]]
    val fh       = new FakeHandler(fn, FakeIOSystem.fakeServerContext)
    val service  = new ServiceServer(fh)
    service.setUpstream(upstream)
    service.connected()
    (service, upstream)
  }

  "ServiceServer" must {

    "process a request" in {
      val (handler, upstream) = fs()
      handler.incoming.push(ByteString("hello")) mustBe PushResult.Ok
      upstream.pipe.pull() mustBe PullResult.Item(ByteString("hello"))
    }

    "return an error response for failed callback" in {
      val (handler, upstream) = fs(x => Callback.failed(new Exception("FAIL")))
      handler.incoming.push(ByteString("hello"))
      upstream.pipe.pull() mustBe PullResult.Item(ByteString("ERROR"))
    }

    "send responses back in the right order" in {
      var promises = Vector[CallbackPromise[ByteString]]()
      val (handler, upstream) = fs(x => {
        val p = new CallbackPromise[ByteString]()
        promises = promises :+ p
        p.callback
      })
      handler.incoming.push(ByteString("AAAA"))
      handler.incoming.push(ByteString("BBBB"))
      promises.size must equal(2)
      promises(1).success(ByteString("B"))
      promises(0).success(ByteString("A"))
      upstream.pipe.pull() mustBe PullResult.Item(ByteString("A"))
      upstream.pipe.pull() mustBe PullResult.Item(ByteString("B"))
    }

    "graceful disconnect allows pending requests to complete" taggedAs (org.scalatest.Tag("test")) in {
      var promises = Vector[CallbackPromise[ByteString]]()
      val t = fakeService(x => {
        val p = new CallbackPromise[ByteString]()
        promises = promises :+ p
        p.callback
      })
      val r1 = ByteString("AAAA")
      t.typedHandler.receivedData(DataBuffer(r1))
      t.typedHandler.disconnect()
      t.status must equal(ConnectionStatus.Connected)
      t.workerProbe.expectNoMsg(100.milliseconds)
      promises(0).success(ByteString("B"))
      t.iterate()
      t.iterate() //second one is needed for disconnect behavior
      t.expectOneWrite(ByteString("B"))
      t.workerProbe.expectMsg(100.milliseconds, WorkerCommand.Disconnect(t.id))
    }

    "timeout request that takes too long" ignore {
      val serverSettings = ServerSettings(
        port = TEST_PORT,
        maxIdleTime = Duration.Inf
      )

    }

    "gracefully handle bad input" in {
      val t = fakeService()
      // this is above  max request size
      t.typedHandler.receivedData(DataBuffer(ByteString("G" * 301)))
      t.iterate()
      t.expectOneWrite(ByteString("ERROR"))
    }

    "properly react to full output buffer" in {
      //the output buffer is set to 2 (see common.scala) so this will force
      //items to stay buffered in the request buffer , but since the request
      //buffer only has size 2, the last item should generate an error
      val (handler, upstream) = fs()
      (1 to 5).foreach { i =>
        handler.incoming.push(ByteString(i.toString))
      }
      (1 to 4).foreach { i =>
        upstream.pipe.pull() mustBe PullResult.Item(ByteString(i.toString))
      }
      upstream.pipe.pull() mustBe PullResult.Item(ByteString("ERROR"))
    }

  }
}
