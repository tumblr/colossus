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

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import org.scalamock.scalatest.MockFactory

import QueuedItem.PostWrite
/*

class ServiceServerSpec extends ColossusSpec with MockFactory {
  val config = ServiceConfig.Default.copy(
    requestBufferSize = 2,
    requestTimeout = 50.milliseconds,
    maxRequestSize = 300.bytes
  )

  class FakeService(handler: ByteString => Callback[ByteString], val serverContext: ServerContext) extends ServiceServer[Raw](config) with HandlerTail{
    def context = serverContext.context

    def processFailure(error: ProcessingFailure[ByteString]) = ByteString("ERROR")

    def processRequest(input: ByteString) = handler(input)

    def testCanPush = upstream.canPush //expose protected method
  }

  def fakeService(handler: ByteString => Callback[ByteString] = x => Callback.successful(x)): TypedMockConnection[PipelineHandler] = {
    val endpoint = MockConnection.server(ctx => Controller(new FakeService(handler, ctx), RawServerCodec))

    endpoint.handler.connected(endpoint)
    endpoint
  }

  def fs(fn: ByteString => Callback[ByteString] = x => Callback.successful(x)): (FakeService, ControllerUpstream[Encoding.Server[Raw]])  = {
    val controllerstub = stub[ControllerUpstream[Encoding.Server[Raw]]]
    val connectionstub = stub[ConnectionManager]
    (connectionstub.isConnected _).when().returns(true)
    (controllerstub.connection _).when().returns(connectionstub)
    (controllerstub.pushFrom _).when(*, *, *).returns(true)
    (controllerstub.canPush _).when().returns(true)
    
    val handler = new FakeService(fn, FakeIOSystem.fakeServerContext)
    handler.setUpstream(controllerstub)
    (handler, controllerstub)
  }

  def expectPush(controller: ControllerUpstream[Encoding.Server[Raw]], message: ByteString) {
      (controller.pushFrom _ ).verify(message, *, *)
  }

  "ServiceServer" must {

    "process a request" in {
      val (handler,upstream) = fs()
      handler.processMessage(ByteString("hello"))
      expectPush(upstream, ByteString("hello"))
    }

    "return an error response for failed callback" in{
      val (handler,upstream) = fs(x => Callback.failed(new Exception("FAIL")))
      handler.processMessage(ByteString("hello"))
      expectPush(upstream, ByteString("ERROR"))
    }

    "send responses back in the right order" in {
      var promises = Vector[CallbackPromise[ByteString]]()
      val (handler,upstream) = fs(x => {
        val p = new CallbackPromise[ByteString]()
        promises = promises :+ p
        p.callback
      })
      handler.processMessage(ByteString("AAAA"))
      handler.processMessage(ByteString("BBBB"))
      promises.size must equal(2)
      promises(1).success(ByteString("B"))      
      promises(0).success(ByteString("A"))
      inSequence {
        expectPush(upstream, ByteString("A"))
        expectPush(upstream, ByteString("B"))
      }
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
*/
