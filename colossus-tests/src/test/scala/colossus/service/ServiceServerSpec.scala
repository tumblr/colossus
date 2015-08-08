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

class ServiceServerSpec extends ColossusSpec {

  import system.dispatcher
  
  class FakeService(handler: ByteString => Callback[ByteString], worker : WorkerRef) extends ServiceServer[ByteString, ByteString](
      config = ServiceConfig (
        name = "/test",
        requestBufferSize = 2,
        requestTimeout = 50.milliseconds
      ),
      worker = worker,
      codec = RawCodec
  ) {
    def processFailure(request: ByteString, reason: Throwable) = ByteString("ERROR:" + reason.toString)

    def processRequest(input: ByteString) = handler(input)

    def receivedMessage(x: Any, s: ActorRef){}
  }

  case class ServiceTest(service: FakeService, endpoint: MockWriteEndpoint, workerProbe: TestProbe)

  def fakeService(handler: ByteString => Callback[ByteString] = x => Callback.successful(x)): ServiceTest = {
    val (probe, worker) = FakeIOSystem.fakeWorkerRef
    val service = new FakeService(handler, worker)
    service.setBind(1, worker)
    val endpoint = new MockWriteEndpoint(100, probe, Some(service)) 
    service.connected(endpoint)
    ServiceTest(service, endpoint, probe)
  }

  "ServiceServer" must {

    "process a request" in {
      val t = fakeService()
      t.service.receivedData(DataBuffer(ByteString("hello")))
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
      t.endpoint.expectNoWrite()
      promises(1).success(ByteString("B"))
      t.endpoint.expectNoWrite()
      promises(0).success(ByteString("A"))
      t.endpoint.expectWrite(ByteString("A"))
      t.endpoint.expectWrite(ByteString("B"))
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
      promises(0).success(ByteString("BBBB"))
      t.endpoint.expectOneWrite(ByteString("BBBB"))
      t.endpoint.status must equal(ConnectionStatus.NotConnected)
    }

    "handle backpressure from output controller" taggedAs(org.scalatest.Tag("test")) in {
      val bytes = ByteString((1 to 100).map{_ => "x"}.mkString)
      def data = DataBuffer(bytes)
      val s = fakeService()
      //this request will fill up the write buffer
      s.service.receivedData(data)
      s.endpoint.expectOneWrite(bytes)
      //this one will fill the partial buffer
      //s.service.receivedData(data)
      //s.endpoint.expectNoWrite()
      //these next two fill up the output controller's buffer (set to 2 in fakeService())
      s.service.outputQueueFull must equal(false)
      s.service.receivedData(data)
      s.service.receivedData(data)
      s.endpoint.expectNoWrite()
      println(s.service.queueSize)
      s.service.outputQueueFull must equal(true)
      //this last one should not even attempt to write and instead keep the
      //response waiting in the request buffer
      s.service.receivedData(data)
      s.endpoint.expectNoWrite()
      s.service.outputQueueFull must equal(true)

      //now as we begin draining the write buffer, both the controller and
      //service layers should begin clearing their buffers
      (1 to 3).foreach { i =>
        println(i)
        s.endpoint.clearBuffer()
        println(s.service.queueSize)
        s.endpoint.expectOneWrite(bytes)
      }
      s.endpoint.expectNoWrite()
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
        val server = Service.serve[Redis](serverSettings, serviceConfig) { context => 
          context.handle{ connection =>
            import connection.callbackExecutor
            connection.become{
              case req => Callback.schedule(500.milliseconds)(Callback.successful(StatusReply("HEllo")))
            }
          }
        }
        withServer(server) {
          val clientConfig = ClientConfig(
            name = "/test-client",
            address = new InetSocketAddress("localhost", TEST_PORT),
            requestTimeout = 500.milliseconds,
            connectionAttempts = PollingDuration.NoRetry
          )
          val client = AsyncServiceClient(clientConfig, new RedisClientCodec)
          Await.result(client.send(Commands.Get(ByteString("foo"))), 1.second) match {
            case e: ErrorReply => {}
            case other => throw new Exception(s"Non-error reply: $other")
          }
        }
      }
    }

    "graceful disconnect" taggedAs(org.scalatest.Tag("test")) in {
      withIOSystem{implicit io => 
        val server = Service.serve[Redis]("test", TEST_PORT, 1.second){_.handle{con => con.become{
          case x if (x.command == "DIE") => {
            con.gracefulDisconnect()
            StatusReply("BYE")
          }
          case other => {
            import con.callbackExecutor
            Callback.schedule(200.milliseconds)(StatusReply("FOO"))
          }
        }}}
        withServer(server) {
          val client = AsyncServiceClient[Redis]("localhost", TEST_PORT, 1.second)
          val r1 = client.send(Command("TEST"))
          val r2 = client.send(Command("DIE"))
          Await.result(r1, 1.second) must equal(StatusReply("FOO"))
          Await.result(r2, 1.second) must equal(StatusReply("BYE"))
        }
      }
    }

  }

  "Streaming Service" must {

    import protocols.http._

    "Serve a basic response as a stream" taggedAs(org.scalatest.Tag("Test")) in {
      withIOSystem{implicit io =>
        val server = Service.become[StreamingHttp]("stream-test", TEST_PORT) {
          case req => StreamingHttpResponse.fromStatic(req.ok("Hello World"))
        }
        withServer(server) { 
          val client = AsyncServiceClient[Http]("localhost", TEST_PORT)
          Await.result(client.send(HttpRequest.get("/")), 1.second).body.get must equal(ByteString("Hello World"))
        }
      }
    }
  }

      

}
