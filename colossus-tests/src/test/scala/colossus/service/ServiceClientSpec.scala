package colossus
package service

import colossus.core._
import testkit._

import akka.testkit.TestProbe

import metrics.MetricAddress

import scala.util.{Success, Failure}
import scala.concurrent.duration._
import akka.util.ByteString
import java.net.InetSocketAddress

import protocols.redis._
import UnifiedProtocol._
import scala.concurrent.Await

import RawProtocol._

class ServiceClientSpec extends ColossusSpec {

  def newClient(failFast: Boolean = false, maxSentSize: Int = 10): (MockWriteEndpoint, ServiceClient[Command,Reply], TestProbe) = {
    val address = new InetSocketAddress("localhost", 12345)
    val (workerProbe, worker) = FakeIOSystem.fakeWorkerRef
    val config = ClientConfig(
      address, 
      10.seconds, 
      MetricAddress.Root / "test", 
      pendingBufferSize = 100, 
      sentBufferSize = maxSentSize, 
      failFast = failFast
    )
    val client = new RedisClient(config, worker)
    client.bind(1, worker)
    client.connect()
    workerProbe.expectMsgType[IOCommand.Connect](50.milliseconds)
    val endpoint = new MockWriteEndpoint(30, workerProbe, Some(client))
    client.connected(endpoint)
    (endpoint, client, workerProbe)
  }

  def sendCommandReplies(client: ServiceClient[Command,Reply], endpoint: MockWriteEndpoint, commandReplies: Map[Command, Reply]) {
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
      bytes = endpoint.clearBuffer()
      parser.decodeAll(DataBuffer.fromByteString(bytes)){command =>
        client.receivedData(commandReplies(command).raw)
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
      endpoint.numWrites must equal(0)
      cb.execute()
      endpoint.numWrites must equal(1)
      endpoint.writeCalls(0) must equal(command.raw)
    }

    "process a reply" in {
      val command = Command(CMD_GET, "foo")
      val reply = BulkReply(ByteString("HELLO"))
      val (endpoint, client, probe) = newClient()
      val cb = client.send(command).map{ r =>
        r must equal (reply)
        endpoint.numWrites must equal(1)
        endpoint.writeCalls(0) must equal(command.raw)
        client.receivedData(reply.raw)
      }.recover{ case t => throw t}

      cb.execute()
    }

    "send big command" in {
      val command = Command(CMD_GET, "123456789012345678901234567890")
      val raw = command.raw
      val (endpoint, client, probe) = newClient()
      val cb = client.send(command)
      cb.execute()
      endpoint.numWrites must equal(1)
      endpoint.writeCalls(0) must equal(raw.slice(0, 30))
      endpoint.clearBuffer() must equal(raw.slice(0,30))
      endpoint.numWrites must equal(2)
      endpoint.writeCalls(1) must equal(raw.slice(30, raw.size))
    }

    "queue a second command when a big command is partially sent" in {
      val command = Command(CMD_GET, "123456789012345678901234567890")
      val command2 = Command(CMD_GET, "hello")
      val raw = command.raw
      val (endpoint, client, probe) = newClient()
      val cb = client.send(command)
      val cb2 = client.send(command2)
      cb.execute()
      cb2.execute()
      endpoint.numWrites must equal(2)
      endpoint.writeCalls(0) must equal(raw.slice(0, 30))
      endpoint.writeCalls(1) must equal(ByteString())
      endpoint.clearBuffer()
      endpoint.numWrites must equal(4)
      endpoint.clearBuffer()
      endpoint.numWrites must equal(5)
      endpoint.writeCalls(2) must equal(raw.slice(30, raw.size))
      endpoint.writeCalls(3) ++ endpoint.writeCalls(4) must equal(command2.raw)

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
      var failed = true
      val cb = client.send(command).map{
        case wat => failed = false
      }
      endpoint.numWrites must equal(0)
      cb.execute()
      endpoint.numWrites must equal(1)
      endpoint.disconnect()
      failed must equal(true)
    }


    "complete pending requests on reconnect" in {
      val command1 = Command(CMD_GET, "123456789012345678901234567890")
      val command2 = Command(CMD_GET, "foo")
      val (endpoint, client, probe) = newClient()
      var failed = true
      val reply = BulkReply(ByteString("foobarbaz"))
      var response: Option[Reply] = None
      val cb1 = client.send(command1).map{
        case wat => failed = false
      }
      val cb2 = client.send(command2).map{
        case r => response = Some(r)
      }
      cb1.execute()
      cb2.execute()
      endpoint.disrupt()
      failed must equal(true)

      val newEndpoint = new MockWriteEndpoint(30, probe, Some(client))
      client.connected(newEndpoint)
      newEndpoint.numWrites must equal(1)
      newEndpoint.clearBuffer()
      client.receivedData(reply.raw)
      response must equal(Some(reply))
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
      endpoint.disconnect()
      failed must equal(true)
      failed2 must equal(true)
    }

    "complete pending requests on buffer clear" in {
      val command1 = Command(CMD_GET, "123456789012345678901234567890")
      val command2 = Command(CMD_GET, "foo")
      val (endpoint, client, probe) = newClient()
      var failed = false
      val reply1 = BulkReply(ByteString("foobarbaz"))
      val reply2 = BulkReply(ByteString("abcdefg")) 
      var response1: Option[Reply] = None
      var response2: Option[Reply] = None

      val cb1 = client.send(command1).execute{
        case Success(r) => response1 = Some(r)
        case Failure(nope) => throw new Exception("NOPE1")
      }
      val cb2 = client.send(command2).execute{
        case Success(r) => response2 = Some(r)
        case Failure(nope) => throw new Exception("NOPE2")
      }
      endpoint.numWrites must equal(2)
      endpoint.writeCalls(1) must equal(ByteString())
      while (endpoint.clearBuffer().size > 0) {}
      client.receivedData(reply1.raw)
      client.receivedData(reply2.raw)
      response1 must equal (Some(reply1))
      response2 must equal (Some(reply2))
    }

    "immediately fail requests when pending buffer is full" in {
      val (endpoint, client, probe) = newClient()
      val big = Command(CMD_GET, "123456789012345678901234567890")
      val commands = (1 to client.config.pendingBufferSize).map{i => 
        Command(CMD_GET, i.toString)
      }
      val shouldFail = Command(CMD_GET, "fail")
      client.send(big).execute()
      commands.foreach{cmd =>
        client.send(cmd).execute{
          case _ => throw new Exception("Executed!")
        }
      }
      endpoint.numWrites must equal (2)
      var failed = false
      client.send(shouldFail).execute{
        case Success(_) => throw new Exception("Didn't fail?!?!")
        case Failure(goodjob) => failed = true
      }

      failed must equal (true)
    }

    "immediately fail request when not connected and failFast is true" in {
      val (endpoint, client, probe) = newClient(true)
      endpoint.disconnect()
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
      endpoint.numWrites must equal(0)
      endpoint.connection_status = ConnectionStatus.NotConnected
      cb.execute{
        case Success(wat) => throw new Exception("NOPE")
        case Failure(yay) => failed = true
      }
      failed must equal(true)
    }

    "buffer requests when sentBuffer reaches max size" in {
      val cmd1 = Command(CMD_GET, "foo")
      val cmd2 = Command(CMD_GET, "bar")
      val rep1 = StatusReply("foo")
      val rep2 = StatusReply("bar")
      var res1: Option[String] = None
      var res2: Option[String] = None
      val (endpoint, client, probe) = newClient(true, 1)
      val cb1 = client.send(cmd1).execute{
        case Success(StatusReply(msg)) => res1 = Some(msg)
        case Failure(nope) => throw nope
        case _ => throw new Exception("Bad Response")
      }
      val cb2 = client.send(cmd2).execute{
        case Success(StatusReply(msg)) => res2 = Some(msg)
        case Failure(nope) => throw nope
        case _ => throw new Exception("Bad Response")
      }
      endpoint.numWrites must equal(1)
      endpoint.clearBuffer()
      client.receivedData(rep1.raw)
      res1 must equal(Some(rep1.message))
      endpoint.numWrites must equal(2)
      endpoint.clearBuffer()
      client.receivedData(rep2.raw)
      res2 must equal(Some(rep2.message))
    }

    "gracefully disconnect" in {
      val cmd1 = Command(CMD_GET, "foo")
      val cmd2 = Command(CMD_GET, "bar")
      val rep1 = StatusReply("foo")
      var res1: Option[String] = None
      val (endpoint, client, probe) = newClient(true, 10)
      val cb1 = client.send(cmd1).execute{
        case Success(StatusReply(msg)) => res1 = Some(msg)
        case Failure(nope) => throw nope
        case _ => throw new Exception("Bad Response")
      }
      endpoint.numWrites must equal(1)
      endpoint.clearBuffer()
      client.gracefulDisconnect()
      endpoint.disconnectCalled must equal(false)
      intercept[CallbackExecutionException] {
        client.send(cmd2).execute{
          case Success(StatusReply(msg)) => {}
          case Failure(nope: NotConnectedException) => throw nope
          case _ => {}
        }
      }
      client.receivedData(rep1.raw)
      res1 must equal(Some(rep1.message))
      endpoint.disconnectCalled must equal(true)
    }

    "graceful disconnect immediately disconnects if there's no outstanding requests" in {
      val (endpoint, client, probe) = newClient(true, 10)
      client.gracefulDisconnect()
      endpoint.disconnectCalled must equal(true)
    }

    "not attempt reconnect if connection is lost during graceful disconnect" in {
      val cmd1 = Command(CMD_GET, "foo")
      val (endpoint, client, probe) = newClient(true, 10)
      client.send(cmd1).execute()
      client.gracefulDisconnect()
      endpoint.connection_status = ConnectionStatus.NotConnected
      endpoint.disrupt()
      probe.expectNoMsg(50.milliseconds)
    }

    //TODO:  I need to be a real test.  I'm confused.  Do I call connect? or connected(endpoint). What is the right API?
    "not allow a disconnected client to reconnect" in {
      val (endpoint, client, probe) = newClient(true, 10)
      endpoint.disconnect()
      val newEndpoint = new MockWriteEndpoint(30, probe, Some(client))
      intercept[StaleClientException]{
        client.connect()
      }
    }

    "not allow a gracefully disconnected client to reconnect" in {
      val (endpoint, client, probe) = newClient(true, 10)
      client.gracefulDisconnect()
      val newEndpoint = new MockWriteEndpoint(30, probe, Some(client))
      intercept[StaleClientException]{
        client.connect()
      }
    }

    "get a shared interface" in {
      val (endpoint, client, probe) = newClient(true, 10)
      val shared = client.shared
      val cmd1 = Command(CMD_GET, "foo")
      val f = shared.send(cmd1)
      //probe.expectMsg(Message(1, AsyncRequest
      probe.expectMsgType[WorkerCommand.Message](50.milliseconds)
    }

    //blocked on https://github.com/tumblr/colossus/issues/19
    "attempts to reconnect when server closes connection" in {
      //try it for real (reacting to a bug with NIO interaction)
      withIOSystem{implicit sys => 
        import service._
        import Response._
        import protocols.redis._

        val reply = StatusReply("LATER LOSER!!!")
        val server = Service.become[Redis]("test", TEST_PORT) {
          case c if (c.command == "BYE") => {
            complete(reply).onWrite(OnWriteAction.Disconnect)
          }
          case other => StatusReply("ok")
        }
        withServer(server) {
          val config = ClientConfig(
            address = new InetSocketAddress("localhost", TEST_PORT),
            name = "/test",
            requestTimeout = 100.milliseconds
          )
          val client = AsyncServiceClient(config, new RedisClientCodec)
          TestClient.waitForConnected(client)
          TestUtil.expectServerConnections(server, 1)
          Await.result(client.send(Command("bye")), 500.milliseconds) must equal(reply)
          Thread.sleep(100)
          TestUtil.expectServerConnections(server, 1)
          TestClient.waitForConnected(client)
          Await.result(client.send(Command("hey")), 500.milliseconds) must equal(StatusReply("ok"))
        }
      }
    }
    "not attempt reconnect when autoReconnect is false" in {
      withIOSystem{ implicit io => 
        val server = Service.become[Raw]("rawwww", TEST_PORT) {
          case foo => foo.onWrite(OnWriteAction.Disconnect)
        }
        withServer(server) {
          val client = TestClient(io, TEST_PORT, connectionAttempts = PollingDuration.NoRetry)
          client.send(ByteString("blah"))
          TestUtil.expectServerConnections(server, 0)
        }
      }
    }

    "attempt to reconnect a maximum amount of times when autoReconnect is true and a maximum amount is specified" in {
      withIOSystem{ implicit io =>
        val server = Service.become[Raw]("rawwww", TEST_PORT) {
          case foo => foo.onWrite(OnWriteAction.Disconnect)
        }
        withServer(server) {

          val config = ClientConfig(
            name = "/test",
            requestTimeout = 100.milliseconds,
            address = new InetSocketAddress("localhost", TEST_PORT + 1),
            connectionAttempts = PollingDuration(50.milliseconds, Some(2L)))

          val client = AsyncServiceClient(config, RawProtocol.RawCodec)(io)
          TestUtil.expectServerConnections(server, 0)
          TestClient.waitForStatus(client, ConnectionStatus.NotConnected)
        }
      }


    }


  }
}


