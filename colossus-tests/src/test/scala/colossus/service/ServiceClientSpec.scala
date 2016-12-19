package colossus
package service

import colossus.core._
import testkit._
import Callback.Implicits._

import akka.testkit.TestProbe

import metrics.MetricAddress

import scala.util.{Success, Failure}
import scala.concurrent.duration._
import akka.util.ByteString
import java.net.InetSocketAddress

import protocols.redis._
import Redis.defaults._
import UnifiedProtocol._
import scala.concurrent.Await
import parsing.DataSize
import DataSize._

import RawProtocol._
import org.scalamock.scalatest.MockFactory

class ServiceClientSpec extends ColossusSpec with MockFactory {

  //TODO: we no longer need to return a triple, just the connection, as the others are members
  def newClient(
    failFast: Boolean = false,
    maxSentSize: Int = 10,
    connectRetry: RetryPolicy = BackoffPolicy(50.milliseconds, BackoffMultiplier.Exponential(5.seconds)),
    requestTimeout: Duration = 10.seconds,
    maxResponseSize: DataSize = 1.MB
  ): (MockConnection, ServiceClient[Redis], TestProbe) = {
    val address = new InetSocketAddress("localhost", 12345)
    val fakeWorker = FakeIOSystem.fakeWorker
    val config = ClientConfig(
      address,
      requestTimeout,
      MetricAddress.Root / "test",
      pendingBufferSize = 100,
      sentBufferSize = maxSentSize,
      failFast = failFast,
      connectRetry = connectRetry,
      maxResponseSize = maxResponseSize
    )
    implicit val w = fakeWorker.worker
    val client = ServiceClient[Redis](config)

    fakeWorker.probe.expectMsgType[WorkerCommand.Bind](100.milliseconds)
    client.setBind()
    fakeWorker.probe.expectMsgType[WorkerCommand.Connect](50.milliseconds)
    val endpoint = MockConnection.client(client, fakeWorker, 30)
    client.connected(endpoint)
    (endpoint, client, fakeWorker.probe)
  }

  def sendCommandReplies(client: ServiceClient[Redis], endpoint: MockConnection, commandReplies: Map[Command, Reply]) {
    var numCalledBack = 0
    commandReplies.foreach{case (command, reply) =>
      client.send(command).map{ r =>
        numCalledBack += 1
        r must equal(reply)
      }.execute()
    }

    val parser = new RedisServerCodec
    var bytes = ByteString()
    do {
      endpoint.iterate()
      bytes = endpoint.clearBuffer()
      parser.decodeAll(DataBuffer.fromByteString(bytes)){command =>
        command match {
          case DecodedResult.Static(cmd) => client.receivedData(commandReplies(cmd).raw)
          case _ => throw new Exception("shouldn't happen, not streaming")
        }

      }
    } while (bytes.size > 0)
    numCalledBack must equal (commandReplies.size)

  }


  "Service Client" must {

    "connect" in {
      val (endpoint, client, probe) = newClient()
      client.connectionStatus must equal (ConnectionStatus.Connected)
    }

    "send a command" in {
      val command = Command(CMD_GET, "foo")
      val (endpoint, client, probe) = newClient()
      val cb = client.send(command)
      endpoint.iterate()
      endpoint.expectNoWrite()
      cb.execute()
      endpoint.iterate()
      endpoint.expectOneWrite(command.raw)
    }

    "process a reply" in {
      val command = Command(CMD_GET, "foo")
      val reply = BulkReply(ByteString("HELLO"))
      val (endpoint, client, probe) = newClient()
      var executed = false
      val cb = client.send(command).map{ r =>
        executed = true
        r must equal (reply)
      }.recover{ case t => throw t}.execute()
      endpoint.iterate()
      endpoint.expectOneWrite(command.raw)
      client.receivedData(reply.raw)
      executed must equal(true)

    }

    "queue a second command when a big command is partially sent"  in {
      val command = Command(CMD_GET, "123456789012345678901234567890")
      val command2 = Command(CMD_GET, "hello")
      val raw = command.raw
      val (endpoint, client, probe) = newClient()
      val cb = client.send(command)
      val cb2 = client.send(command2)
      cb.execute()
      cb2.execute()
      endpoint.iterate()
      endpoint.expectOneWrite(raw.take(endpoint.maxWriteSize))
      endpoint.clearBuffer()
      endpoint.iterate()
      val remain = raw.drop(endpoint.maxWriteSize) ++ command2.raw
      endpoint.expectOneWrite(remain.take(endpoint.maxWriteSize))
      endpoint.clearBuffer()
      endpoint.iterate()
      endpoint.expectOneWrite(remain.drop(endpoint.maxWriteSize))
      endpoint.clearBuffer()
      endpoint.iterate()
      endpoint.expectNoWrite()

    }

    "get the right replies when commands are buffered" in {
      val commandReplies = Map(
        Command(CMD_GET, "foo") -> BulkReply(ByteString("foo")),
        Command(CMD_GET, "123456789012345678901234567890") -> BulkReply(ByteString("big")),
        Command(CMD_DEL, "bar") -> IntegerReply(1)

      )
      val (endpoint, client, probe) = newClient()

      sendCommandReplies(client, endpoint, commandReplies)

      val cr2 = Map(
        Command(CMD_GET, "bar") -> NilReply,
        Command(CMD_SET, "foo", "baz") -> StatusReply("OK")
      )
      sendCommandReplies(client, endpoint, cr2)
    }

    "fails requests in transit when connection closes" in {
      val command = Command(CMD_GET, "foo")
      val (endpoint, client, probe) = newClient()
      var failed = false
      val cb = client.send(command).map{
        case wat => failed = false
      }.recover{
        case _ => failed = true
      }
      endpoint.expectNoWrite()
      cb.execute()
      endpoint.iterate()
      endpoint.expectOneWrite(command.raw)
      endpoint.disrupt()
      failed must equal(true)
    }



    "fail pending requests on disconnect with failFast" in {
      val command1 = Command(CMD_GET, "123456789012345678901234567890")
      val command2 = Command(CMD_GET, "foo")
      val (endpoint, client, probe) = newClient(true)
      var failed = false
      var failed2 = false
      val cb1 = client.send(command1).execute{
        case Success(wat) => throw new Exception("NOPE1")
        case Failure(yay) => failed = true
      }
      val cb2 = client.send(command2).execute{
        case Success(wat) => throw new Exception("NOPE2")
        case Failure(yay) => failed2 = true
      }
      endpoint.disrupt()
      failed must equal(true)
      failed2 must equal(true)
    }

    //this should be written as a controller test
    "immediately fail requests when pending buffer is full" in {
      val (endpoint, client, probe) = newClient()
      val big = Command(CMD_GET, "hello")
      val commands = (1 to client.config.pendingBufferSize).map{i =>
        Command(CMD_GET, i.toString)
      }
      val shouldFail = Command(CMD_GET, "fail")
      client.send(big).execute()
      commands.foreach{cmd =>
        client.send(cmd).execute()
      }
      var failed = false
      client.send(shouldFail).execute{
        case Success(_) => throw new Exception("Didn't fail?!?!")
        case Failure(goodjob) => failed = true
      }
      failed must equal (true)
    }

    "immediately fail request when not connected and failFast is true" in {
      val (endpoint, client, probe) = newClient(true)
      endpoint.disrupt()
      val shouldFail = Command(CMD_GET, "fail")
      var failed = false
      client.send(shouldFail).execute{
        case Failure(_) => failed = true
        case _ => throw new Exception("NO")
      }
      failed must equal (true)
    }


    "immediately fail request on write attempt when ConnectionClosed is thrown and failFast is true" in {
      val command = Command(CMD_GET, "foo")
      val (endpoint, client, probe) = newClient(true)
      var failed = false
      val cb = client.send(command)
      endpoint.expectNoWrite()
      endpoint.disrupt()
      cb.execute{
        case Success(wat) => println("HERE");throw new Exception("NOPE")
        case Failure(yay) => println("THER");failed = true
      }
      failed must equal(true)
    }

    "not overflow sentBuffer when draining from pending" in {
      // here we're checking to make sure that if we've previously paused
      // writes, and then resume writing, that we continue to respect the max
      // sentBuffer size.
      val cmd = Command(CMD_GET, "foo")
      val (endpoint, client, probe) = newClient(true, 1)
      val cmds = (0 to 3).map{i =>
        client.send(cmd).execute()
      }
      val reply = StatusReply("foo")
      (0 to 3).map{i =>
        endpoint.iterate()
        endpoint.expectOneWrite(cmd.raw)
        endpoint.clearBuffer()
        client.receivedData(reply.raw)
      }
    }

    "graceful disconnect allows outstanding request to complete" in {
      val cmd1 = Command(CMD_GET, "foo")
      val rep1 = StatusReply("foo")
      var res1: Option[String] = None
      val (endpoint, client, probe) = newClient(true, 10)
      val cb1 = client.send(cmd1).execute{
        case Success(StatusReply(msg)) => res1 = Some(msg)
        case Failure(nope) => throw nope
        case _ => throw new Exception("Bad Response")
      }
      client.disconnect()
      endpoint.iterate()
      endpoint.expectOneWrite(cmd1.raw)
      endpoint.clearBuffer()
      endpoint.disconnectCalled must equal(false)
      probe.expectNoMsg(100.milliseconds)
      client.receivedData(rep1.raw)
      res1 must equal(Some(rep1.message))
      probe.expectMsg(100.milliseconds, WorkerCommand.Disconnect(client.id))
    }

    "graceful disconnect rejects new requests while disconnecting" in {
      val (endpoint, client, probe) = newClient(true, 10)
      client.send(Command("BLAH")).execute()
      client.disconnect()
      endpoint.disconnectCalled must equal(false)
      intercept[CallbackExecutionException] {
        client.send(Command("BLEH")).execute{
          case Success(StatusReply(msg)) => {}
          case Failure(nope: NotConnectedException) => throw nope
          case _ => {}
        }
      }
    }

    "graceful disconnect immediately disconnects if there's no outstanding requests" in {
      val (endpoint, client, probe) = newClient(true, 10)
      client.disconnect()
      probe.expectMsg(100.milliseconds, WorkerCommand.Disconnect(client.id))
    }

    "not attempt reconnect if connection is lost during graceful disconnect" in {
      val cmd1 = Command(CMD_GET, "foo")
      val (endpoint, client, probe) = newClient(true, 10)
      client.send(cmd1).execute()
      client.disconnect()
      probe.expectNoMsg(100.milliseconds)
      endpoint.disrupt()
      probe.expectMsg(100.milliseconds, WorkerCommand.UnbindWorkerItem(client.id))
    }

    "unbind from the worker when not attempting to reconnect" in {
      val (endpoint, client, probe) = newClient(true, 10, connectRetry = NoRetry)
      endpoint.disrupt()
      probe.expectMsg(100.milliseconds, WorkerCommand.UnbindWorkerItem(client.id))
    }

    "graceful disconnect inside a callback" in {
      val (endpoint, client, probe) = newClient(true, 10, connectRetry = NoRetry)
      val cmd = Command("BAH")
      val reply = StatusReply("WAT")
      client.send(Command("BAH")).map{r =>
        client.disconnect()
        r
      }.execute()
      endpoint.iterate()
      endpoint.expectOneWrite(cmd.raw)
      probe.expectNoMsg(100.milliseconds)
      client.receivedData(reply.raw)
      probe.expectMsg(100.milliseconds, WorkerCommand.Disconnect(client.id))
    }

    "attempts to reconnect when server closes connection" in {
      //try it for real (reacting to a bug with NIO interaction)
      withIOSystem{implicit sys =>
        import protocols.redis._

        val reply = StatusReply("LATER LOSER!!!")
        val server = Server.basic("test", TEST_PORT)( new Service[Redis](_) { def handle = {
          case c if (c.command == "BYE") => {
            disconnect()
            reply
          }
          case other => {
            StatusReply("ok")
          }
        }})
        withServer(server) {
          val config = ClientConfig(
            address = new InetSocketAddress("localhost", TEST_PORT),
            name = "/test",
            requestTimeout = 1.second
          )
          val client = FutureClient[Redis](config)
          TestClient.waitForConnected(client)
          TestUtil.expectServerConnections(server, 1)
          Await.result(client.send(Command("bye")), 500.milliseconds) must equal(reply)
          TestUtil.expectServerConnections(server, 1)
          TestClient.waitForConnected(client)
          Await.result(client.send(Command("00000000000")), 500.milliseconds) must equal(StatusReply("ok"))
        }
      }
    }


    "not attempt reconnect when autoReconnect is false" in {
      withIOSystem{ implicit io =>
        val server = Server.basic("rawwww", TEST_PORT)( new Service[Raw](_){ def handle = {
          case foo => {
            disconnect()
            foo
          }
        }})
        withServer(server) {
          val client = TestClient(io, TEST_PORT, connectRetry = NoRetry)
          import server.system.actorSystem.dispatcher
          client.send(ByteString("blah")).onComplete(println)
          TestUtil.expectServerConnections(server, 0)
        }
      }
    }

    "attempt to reconnect a maximum amount of times when autoReconnect is true and a maximum amount is specified" in {
      withIOSystem{ implicit io =>
        val server = Server.basic("rawwww", TEST_PORT)( new Service[Raw](_){ def handle = {
          case foo => {
            disconnect()
            foo
          }
        }})
        withServer(server) {

          val config = ClientConfig(
            name = "/test",
            requestTimeout = 100.milliseconds,
            address = new InetSocketAddress("localhost", TEST_PORT + 1),
            connectRetry = BackoffPolicy(50.milliseconds, BackoffMultiplier.Exponential(5.seconds), maxTries = Some(2))
          )

          val client = FutureClient[Raw](config)
          TestUtil.expectServerConnections(server, 0)
          TestClient.waitForStatus(client, ConnectionStatus.NotConnected)
        }
      }
    }

    "not try to reconnect if disconnect is called while failing to connect" in {
      val fakeWorker = FakeIOSystem.fakeWorker
      implicit val w = fakeWorker.worker
      val client = ServiceClient[Raw]("localhost", TEST_PORT, 1.second)

      fakeWorker.probe.expectMsgType[WorkerCommand.Bind](100.milliseconds)
      client.setBind()
      fakeWorker.probe.expectMsgType[WorkerCommand.Connect](50.milliseconds)

      client.connectionTerminated(DisconnectCause.ConnectFailed(new Exception("HI!!")))
      fakeWorker.probe.expectMsgType[WorkerCommand.Connect](50.milliseconds)

      client.disconnect()
      //no disconnect message is sent because it's not connected
      fakeWorker.probe.expectNoMsg(50.milliseconds)

      client.connectionTerminated(DisconnectCause.ConnectFailed(new Exception("HI!!")))
      fakeWorker.probe.expectMsg(50.milliseconds, WorkerCommand.UnbindWorkerItem(client.id))
      fakeWorker.probe.expectNoMsg(50.milliseconds)

    }

    "shutdown the connection when an in-flight request times out" in {
      val command = Command(CMD_GET, "foo")
      val (endpoint, client, probe) = newClient(requestTimeout = 10.milliseconds, connectRetry = NoRetry)
      var failed = true
      val cb = client.send(command).map{
        case wat => failed = false
      }
      cb.execute()
      endpoint.iterate()
      endpoint.expectOneWrite(command.raw)

      Thread.sleep(150)
      client.idleCheck(100.milliseconds)

      probe.expectMsg(500.milliseconds, WorkerCommand.Kill(client.id, DisconnectCause.TimedOut))

      failed must equal(true)
    }

    "kill the connection if a parse exception occurs" in {
      val (endpoint, client, probe) = newClient(requestTimeout = 10.milliseconds, connectRetry = NoRetry, maxResponseSize = DataSize(1))
      val cb = client.send(Command(CMD_GET, "foo")).execute()
      endpoint.iterate()
      client.receivedData(StatusReply("uhoh").raw)
      val msg = probe.receiveOne(500.milliseconds)
      msg mustBe a[WorkerCommand.Kill]
    }


    "timeout requests while waiting to reconnect" taggedAs(org.scalatest.Tag("test")) in {
      withIOSystem{ implicit io =>
        val config = ClientConfig(
          name = "/test",
          requestTimeout = 100.milliseconds,
          address = new InetSocketAddress("localhost", TEST_PORT),
          failFast = false,
          connectRetry = BackoffPolicy(10.seconds, BackoffMultiplier.Constant)
        )
        val client = Raw.futureClient(config)
        import io.actorSystem.dispatcher
        val f = client.send(ByteString("blah"))
        Thread.sleep(350)
        //beware, a java TimeoutException is NOT what we want, that is simply
        //the future timing out, which it shouldn't here
        intercept[RequestTimeoutException] {
          Await.result(f, 10.seconds)
        }
      }
    }

    "work with mocking" in {
      import protocols.http._

      val c = stub[HttpClient[Callback]]

      val resp = HttpRequest.get("/foo").ok("hello")

      (c.send _) when(HttpRequest.get("/foo")) returns(Callback.successful(resp))

      implicit val executor = FakeIOSystem.testExecutor
      CallbackAwait.result(c.send(HttpRequest.get("/foo")), 1.second) mustBe resp

    }


  }
}

