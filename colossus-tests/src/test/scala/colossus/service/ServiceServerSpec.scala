package colossus
package service

import testkit._
import core._
import Callback.Implicits._

import scala.concurrent.duration._
import akka.actor.ActorRef
import akka.testkit.TestProbe
import akka.util.ByteString
import java.net.InetSocketAddress

import protocols.redis._
import scala.concurrent.Await

import RawProtocol._

import scala.util.{Failure, Success, Try}

class ServiceServerSpec extends ColossusSpec {

  import system.dispatcher
  
  class FakeService(handler: ByteString => Callback[ByteString], worker : WorkerRef) extends ServiceServer[ByteString, ByteString](
      config = ServiceConfig (
        name = "/test",
        requestBufferSize = 2,
        requestTimeout = 50.milliseconds
      ),
      codec = RawCodec
  )(worker.system) {
    def processFailure(request: ByteString, reason: Throwable) = ByteString("ERROR:" + reason.toString)

    def processRequest(input: ByteString) = handler(input)

    def receivedMessage(x: Any, s: ActorRef){}

    def testCanPush = canPush //expose protected method
  }

  case class ServiceTest(service: FakeService, endpoint: MockConnection, workerProbe: TestProbe)

  def fakeService(handler: ByteString => Callback[ByteString] = x => Callback.successful(x)): ServiceTest = {
    val fw = FakeIOSystem.fakeWorker
    val service = new FakeService(handler, fw.worker)
    service.setBind(1, fw.worker)
    val endpoint = MockConnection.server(service)
    service.connected(endpoint)
    ServiceTest(service, endpoint, fw.probe)
  }

  "ServiceServer" must {

    "process a request" in {
      val t = fakeService()
      t.service.receivedData(DataBuffer(ByteString("hello")))
      t.endpoint.iterate()
      t.endpoint.expectOneWrite(ByteString("hello"))
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
      t.service.receivedData(DataBuffer(r1))
      t.service.receivedData(DataBuffer(r2))
      promises.size must equal(2)
      t.endpoint.iterate()
      t.endpoint.expectNoWrite()
      promises(1).success(ByteString("B"))
      t.endpoint.iterate()
      t.endpoint.expectNoWrite()
      promises(0).success(ByteString("A"))
      t.endpoint.iterate()
      t.endpoint.expectOneWrite(ByteString("AB"))
    }

    "graceful disconnect allows pending requests to complete" in {
      var promises = Vector[CallbackPromise[ByteString]]()
      val t = fakeService(x => {
        val p = new CallbackPromise[ByteString]()
        promises = promises :+ p
        p.callback
      })
      val r1 = ByteString("AAAA")
      t.service.receivedData(DataBuffer(r1))
      t.service.gracefulDisconnect()
      t.endpoint.readsEnabled must equal(false)
      t.endpoint.status must equal(ConnectionStatus.Connected)
      t.endpoint.workerProbe.expectNoMsg(100.milliseconds)
      promises(0).success(ByteString("BBBB"))
      t.endpoint.iterate()
      t.endpoint.expectOneWrite(ByteString("BBBB"))
      t.endpoint.workerProbe.expectMsg(100.milliseconds, WorkerCommand.Disconnect(t.service.id.get))
    }

    "handle backpressure from output controller" in {
      var promises = Vector[CallbackPromise[ByteString]]()
      val s = fakeService(x => {
        val p = new CallbackPromise[ByteString]()
        promises = promises :+ p
        p.callback
      })

      //this request will fill up the write buffer
      val big = ByteString(List.fill(controller.OutputController.DefaultDataBufferSize * 2)("b").mkString)
      s.service.receivedData(DataBuffer(ByteString("ASDF")))
      promises(0).success(big)
      s.service.testCanPush must equal(true)


      //these next two fill up the output controller's message queue (set to 2 in fakeService())
      s.service.receivedData(DataBuffer(ByteString("G")))
      s.service.receivedData(DataBuffer(ByteString("H")))
      promises(1).success(ByteString("A"))
      promises(2).success(ByteString("B"))
      s.service.testCanPush must equal(false)
      s.service.currentRequestBufferSize must equal(0)

      //this last one should not even attempt to write and instead keep the
      //response waiting in the request buffer
      s.service.receivedData(DataBuffer(ByteString("I")))
      promises(3).success(ByteString("C"))
      s.service.currentRequestBufferSize must equal(1)
      s.service.testCanPush must equal(false)

      //now as we begin draining the write buffer, both the controller and
      //service layers should begin clearing their buffers
      s.endpoint.iterateAndClear()

      s.service.testCanPush must equal(true)
      s.service.currentRequestBufferSize must equal(0)
    }


    "timeout request that takes too long" in {
      val serverSettings = ServerSettings (
        port = TEST_PORT,
        maxIdleTime = Duration.Inf
      )

      val serviceConfig = ServiceConfig[Redis#Input, Redis#Output] (
        name = "/timeout-test",
        requestTimeout = 50.milliseconds
      )
      withIOSystem{implicit io => 
        val server = Server.start("test", serverSettings) { context => 
          import context.worker.callbackExecutor
          context onConnect { connection =>
            connection accept new Service[Redis]{ def handle = {
              case req => Callback.schedule(500.milliseconds)(Callback.successful(StatusReply("HEllo")))
            }}
          }
        }
        withServer(server) {
          val clientConfig = ClientConfig(
            name = "/test-client",
            address = new InetSocketAddress("localhost", TEST_PORT),
            requestTimeout = 500.milliseconds,
            connectionAttempts = PollingDuration.NoRetry
          )
          val client = new RedisFutureClient(AsyncServiceClient(clientConfig, new RedisClientCodec))
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
