package colossus
package service

import testkit._
import core._

import scala.concurrent.duration._
import akka.actor.ActorRef
import akka.testkit.TestProbe
import akka.util.ByteString
import java.net.InetSocketAddress

import protocols.redis._
import Redis.defaults._
import scala.concurrent.Await

import RawProtocol._

import scala.util.{Failure, Success, Try}

class ServiceServerSpec extends ColossusSpec {

  import system.dispatcher
  
  class FakeService(handler: ByteString => Callback[ByteString], srv: ServerContext) extends ServiceServer[ByteString, ByteString](
      config = ServiceConfig (
        requestBufferSize = 2,
        requestTimeout = 50.milliseconds
      ),
      codec = RawCodec,
      serverContext = srv
  ) {

    def processFailure(error: ProcessingFailure[ByteString]) = ByteString("ERROR")

    def processRequest(input: ByteString) = handler(input)

    def receivedMessage(x: Any, s: ActorRef){}

    def testCanPush = canPush //expose protected method
  }

  def fakeService(handler: ByteString => Callback[ByteString] = x => Callback.successful(x)): TypedMockConnection[FakeService] = {
    val endpoint = MockConnection.server(new FakeService(handler, _))

    endpoint.handler.connected(endpoint)
    endpoint
  }

  "ServiceServer" must {

    "process a request" in {
      val t = fakeService()
      t.handler.receivedData(DataBuffer(ByteString("hello")))
      t.iterate()
      t.expectOneWrite(ByteString("hello"))
    }

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


    "timeout request that takes too long" in {
      val serverSettings = ServerSettings (
        port = TEST_PORT,
        maxIdleTime = Duration.Inf
      )

      val serviceConfig = ServiceConfig (
        requestTimeout = 50.milliseconds
      )
      withIOSystem{implicit io => 
        val server = Server.basic("test", serverSettings) { new Service[Redis](_){ 
          def handle = {
              case req => Callback.schedule(500.milliseconds)(Callback.successful(StatusReply("HEllo")))
          }
        }}
        withServer(server) {
          val clientConfig = ClientConfig(
            name = "/test-client",
            address = new InetSocketAddress("localhost", TEST_PORT),
            requestTimeout = 800.milliseconds,
            connectionAttempts = PollingDuration.NoRetry
          )
          val client = Redis.futureClient(clientConfig)
          val t = Try {
            Await.result(client.get(ByteString("foo")), 1.second)
          }
          t match {
            case Success(x) => throw new Exception(s"Non-error reply: $x")
            case Failure(ex) =>{}
          }
        }
      }
    }

  }

}
