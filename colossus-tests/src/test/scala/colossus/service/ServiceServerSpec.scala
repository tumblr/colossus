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

class ServiceServerSpec extends ColossusSpec with MockFactory {
  val config = ServiceConfig.Default.copy(
    requestBufferSize = 2,
    requestTimeout = 50.milliseconds,
    maxRequestSize = 300.bytes
  )

  class FakeService(handler: ByteString => Callback[ByteString], srv: ServerContext) extends ServiceServer[Raw](config){
    val serverContext = srv
    val context = srv.context

    def processFailure(error: ProcessingFailure[ByteString]) = ByteString("ERROR")

    def processRequest(input: ByteString) = handler(input)

    def testCanPush = upstream.canPush //expose protected method
  }

  /*
  def fakeService(handler: ByteString => Callback[ByteString] = x => Callback.successful(x)): TypedMockConnection[PipelineHandler] = {
    val endpoint = MockConnection.server(ctx => Controller(new FakeService(handler, ctx), RawServerCodec))

    endpoint.handler.connected(endpoint)
    endpoint
  }
  */

  def fs(fn: ByteString => Callback[ByteString] = x => Callback.successful(x)): (FakeService, ControllerUpstream[Raw#ServerEncoding])  = {
    val thestub = stub[ControllerUpstream[Raw#ServerEncoding]]
    
    val handler = new FakeService(fn, FakeIOSystem.fakeServerContext)
    handler.setUpstream(thestub)
    (handler, thestub)
  }

  "ServiceServer" must {

    "process a request" in {
      val (h,s) = fs()
      h.processRequest(ByteString("hello"))
      (s.push _).expects(ByteString("hello"), * )
    }
    /*

    "return an error response for failed callback" in{
      val t = fakeService(x => Callback.failed(new Exception("FAIL")))
      t.handler.receivedData(DataBuffer(ByteString("hello")))
      t.iterate()
      t.expectOneWrite(ByteString("ERROR"))

    }

    "send responses back in the right order" in {
      var promises = Vector[CallbackPromise[ByteString]]()
      val t = fakeService(x => {
        val p = new CallbackPromise[ByteString]()
        promises = promises :+ p
        p.callback
      })
      val r1 = ByteString("AAAA")
      val r2 = ByteString("BBBB")
      t.typedHandler.receivedData(DataBuffer(r1))
      t.typedHandler.receivedData(DataBuffer(r2))
      promises.size must equal(2)
      t.iterate()
      t.expectNoWrite()
      promises(1).success(ByteString("B"))
      t.iterate()
      t.expectNoWrite()
      promises(0).success(ByteString("A"))
      t.iterate()
      t.expectOneWrite(ByteString("AB"))
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
      t.readsEnabled must equal(false)
      t.status must equal(ConnectionStatus.Connected)
      t.workerProbe.expectNoMsg(100.milliseconds)
      promises(0).success(ByteString("B"))
      t.iterate()
      t.iterate() //second one is needed for disconnect behavior
      t.expectOneWrite(ByteString("B"))
      t.workerProbe.expectMsg(100.milliseconds, WorkerCommand.Disconnect(t.id))
    }

    "handle backpressure from output controller" in {
      var promises = Vector[CallbackPromise[ByteString]]()
      val s = fakeService(x => {
        val p = new CallbackPromise[ByteString]()
        promises = promises :+ p
        p.callback
      })

      //these next two fill up the output controller's message queue (set to 2 in fakeService())
      s.typedHandler.receivedData(DataBuffer(ByteString("G")))
      s.typedHandler.receivedData(DataBuffer(ByteString("H")))
      promises(0).success(ByteString("A"))
      promises(1).success(ByteString("B"))
      s.typedHandler.testCanPush must equal(false)
      s.typedHandler.currentRequestBufferSize must equal(0)

      //this last one should not even attempt to write and instead keep the
      //response waiting in the request buffer
      s.typedHandler.receivedData(DataBuffer(ByteString("I")))
      promises(2).success(ByteString("C"))
      s.typedHandler.currentRequestBufferSize must equal(1)
      s.typedHandler.testCanPush must equal(false)

      //now as we begin draining the write buffer, both the controller and
      //service layers should begin clearing their buffers
      s.iterateAndClear()

      s.typedHandler.testCanPush must equal(true)
      s.typedHandler.currentRequestBufferSize must equal(0)
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

    */
  }
}
