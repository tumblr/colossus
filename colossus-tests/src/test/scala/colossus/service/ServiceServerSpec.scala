package colossus
package service

import java.net.InetSocketAddress

import akka.actor.ActorRef
import akka.util.ByteString
import colossus.RawProtocol._
import colossus.core._
import controller._
import colossus.parsing.DataSize._
import colossus.testkit._
import streaming._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import org.scalamock.scalatest.MockFactory

class ServiceServerSpec extends ColossusSpec with MockFactory with ControllerMocks {
  val config = ServiceConfig.Default.copy(
    requestBufferSize = 2,
    requestTimeout = 50.milliseconds,
    maxRequestSize = 300.bytes
  )

  class FakeService(handler: ByteString => Callback[ByteString], val serverContext: ServerContext) extends ServiceServer[Raw](config) with HandlerTail{
    def context = serverContext.context

    def processFailure(error: ProcessingFailure[ByteString]) = ByteString("ERROR")

    def processRequest(input: ByteString) = handler(input)

  }

  def fakeService(handler: ByteString => Callback[ByteString] = x => Callback.successful(x)): TypedMockConnection[PipelineHandler] = {
    val endpoint = MockConnection.server(ctx => Controller(new FakeService(handler, ctx), RawServerCodec))

    endpoint.handler.connected(endpoint)
    endpoint
  }

  def fs(fn: ByteString => Callback[ByteString] = x => Callback.successful(x)): (FakeService, TestUpstream[Encoding.Server[Raw]])  = {
    val upstream = new TestUpstream[Encoding.Server[Raw]]
    val handler = new FakeService(fn, FakeIOSystem.fakeServerContext)
    handler.setUpstream(upstream)
    handler.connected()
    (handler, upstream)
  }

  "ServiceServer" must {

    "process a request" in {
      val (handler,upstream) = fs()
      handler.incoming.push(ByteString("hello")) mustBe PushResult.Ok
      upstream.pipe.pull() mustBe PullResult.Item(ByteString("hello"))
    }

    "return an error response for failed callback" in{
      val (handler,upstream) = fs(x => Callback.failed(new Exception("FAIL")))
      handler.incoming.push(ByteString("hello"))
      upstream.pipe.pull() mustBe PullResult.Item(ByteString("ERROR"))
    }

    "send responses back in the right order" in {
      var promises = Vector[CallbackPromise[ByteString]]()
      val (handler,upstream) = fs(x => {
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

    "graceful disconnect allows pending requests to complete" taggedAs(org.scalatest.Tag("test")) in {
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
      import colossus.protocols.redis._
      import colossus.protocols.redis.server._
      val serverSettings = ServerSettings (
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

  }
}
