package colossus.core
package server

import java.util.concurrent.atomic.AtomicInteger

import colossus.metrics.MetricSystem
import colossus.testkit.{ColossusSpec, FakeIOSystem}
import akka.actor._
import akka.testkit.TestProbe

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.util.ByteString
import colossus._

class ServerSpec extends ColossusSpec {

  def expectConnections(server: ServerRef, num: Int) {
    server.server ! Server.GetInfo
    expectMsg(50.milliseconds, Server.ServerInfo(num, ServerStatus.Bound))
  }

  "IOSystem" must {
    "startup and shutdown" in {
      val io = IOSystem("test", Some(2), MetricSystem.deadSystem)
      Thread.sleep(50)
      io ! WorkerManager.ReadyCheck
      val probe = TestProbe()
      probe watch io.workerManager
      expectMsgClass(50.milliseconds, classOf[WorkerManager.WorkersReady])
      io.shutdown()
      probe.expectTerminated(io.workerManager)
    }

    "list all registered servers" in {
      withIOSystem { implicit io =>
        implicit val ec = io.actorSystem.dispatcher
        val server1     = Server.basic("echo1", TEST_PORT)(context => new EchoHandler(context))
        waitForServer(server1)
        val server2 = Server.basic("echo2", TEST_PORT + 1)(context => new EchoHandler(context))
        waitForServer(server2)
        val servers = Await.result(io.registeredServers, 200 milliseconds)
        servers must have length 2
        servers.map(_.name.toString) mustBe Seq("/echo1", "/echo2")

        server2.shutdown()
        Thread.sleep(100)

        val remainingServers = Await.result(io.registeredServers, 200 milliseconds)
        remainingServers must have length 1
        remainingServers.head.name.toString mustBe "/echo1"
      }
    }

    "expose ConnectionSummary data" in {
      val settings = ServerSettings(
        port = TEST_PORT
      )

      withServer(serverContext => new EchoHandler(serverContext)) { server =>
        {
          import scala.concurrent.ExecutionContext.Implicits.global

          val c1 = TestClient(server.system, TEST_PORT)
          expectConnections(server, 1)
          val c2  = TestClient(server.system, TEST_PORT)
          val res = Await.result(server.system.connectionSummary, 2000.milliseconds)
          res.infos.size mustBe 4 //2 connections for each client since there are both clients and servers
          res.infos.count(_.domain == "client") mustBe 2
          res.infos.count(_.domain == "/test-server") mustBe 2
          c1.disconnect()
          c2.disconnect()
        }
      }
    }
  }

  "Server" must {
    "attach to a system and start" in {
      withIOSystem { implicit io =>
        val server = Server.basic("echo", TEST_PORT)(context => new EchoHandler(context))
        waitForServer(server)
        val c    = TestClient(io, TEST_PORT)
        val data = ByteString("hello world!")
        Await.result(c.send(data), 100.milliseconds) must equal(data)
      }
    }

    "shutting down system shuts down attached servers" in {
      implicit val io = IOSystem("test", Some(2), MetricSystem.deadSystem)
      val probe       = TestProbe()
      probe watch io.workerManager
      val server = Server.basic("echo", TEST_PORT)(context => new EchoHandler(context))
      val probe2 = TestProbe()
      probe2 watch server.server
      Thread.sleep(100)
      io.shutdown()
      probe2.expectTerminated(server.server)
      probe.expectTerminated(io.workerManager)
    }

    "shutdown when it cannot bind to a port when a duration is supplied" in {
      withIOSystem { implicit io =>
        val existingServer = Server.basic("echo3", TEST_PORT)(context => new EchoHandler(context))
        waitForServer(existingServer)
        val settings = ServerSettings(port = TEST_PORT,
                                      bindingRetry =
                                        BackoffPolicy(50 milliseconds, BackoffMultiplier.Constant, maxTries = Some(3)))
        val p                         = TestProbe()
        val clashingServer: ServerRef = Server.basic("echo2", settings)(context => new EchoHandler(context))
        p.watch(clashingServer.server)
        p.expectTerminated(clashingServer.server)
      }
    }

    "shutdown when a initializer fails to instantiate" in {
      val badDelegator: Initializer.Factory = (_: InitContext) => throw new Exception("failed during initializer creation")

      withIOSystem { implicit io =>
        val cfg = ServerConfig(
          "echo",
          badDelegator,
          ServerSettings(
            TEST_PORT,
            delegatorCreationPolicy =
              WaitPolicy(200 milliseconds,
                         BackoffPolicy(50 milliseconds, BackoffMultiplier.Constant, maxTries = Some(3)))
          )
        )
        val serverProbe  = TestProbe()
        val failedServer = Server(cfg)
        serverProbe.watch(failedServer.server)
        serverProbe.expectTerminated(failedServer.server)
      }
    }

    //note in this test the server is killed with PoisonPill, not its own Shutdown message
    "shutdown all associated connections when killed" in {
      withIOSystem { implicit io =>
        val server = Server.basic("echo", TEST_PORT)(context => new EchoHandler(context))
        withServer(server) {
          val client = TestClient(io, TEST_PORT, connectRetry = NoRetry)
          server.server ! PoisonPill
          TestClient.waitForStatus(client, ConnectionStatus.NotConnected)
        }
      }
    }

    "shutdown with Shutdown message" in {
      withIOSystem { implicit io =>
        val server = Server.basic("echo", TEST_PORT)(context => new EchoHandler(context))
        //spin up a client just to make sure the server is running
        withServer(server) {
          val client = TestClient(io, TEST_PORT, connectRetry = NoRetry)
          val probe  = TestProbe()
          probe.watch(server.server)
          server.server ! Server.Shutdown
          probe.expectTerminated(server.server, 2.seconds)
          TestClient.waitForStatus(client, ConnectionStatus.NotConnected)
        }
      }
    }

    "signal open connections before termination when shutdown" in {
      val probe = TestProbe()
      class MyHandler(c: ServerContext) extends NoopHandler(c) {
        override def onShutdown() { probe.ref ! "SHUTDOWN" }
        override def connectionTerminated(cause: DisconnectCause) {
          probe.ref ! "TERMINATED"
        }
      }
      withServer(serverContext => new MyHandler(serverContext)) { server =>
        val client = TestClient(server.system, TEST_PORT, connectRetry = NoRetry)
        server.server ! Server.Shutdown
        probe.expectMsg(2.seconds, "SHUTDOWN")
        probe.expectMsg(2.seconds, "TERMINATED")
      }
    }

    "immediately terminate when last open connection closes" in {
      withIOSystem { implicit io =>
        val probe = TestProbe()
        val server =
          Server.basic("test", ServerSettings(port = TEST_PORT, shutdownTimeout = 1.hour))(
            serverContext => new EchoHandler(serverContext)
          )
        probe.watch(server.server)
        withServer(server) {
          val client = TestClient(io, TEST_PORT, connectRetry = NoRetry)
          server.server ! Server.Shutdown
          TestClient.waitForStatus(client, ConnectionStatus.NotConnected)
          probe.expectTerminated(server.server, 2.seconds)
        }
      }
    }

    "shutdown when a initializer surpasses the allotted duration" in {
      withIOSystem { implicit io =>
        val serverProbe = TestProbe()
        val failedServer =
          Server.start("fail",
                       ServerSettings(TEST_PORT, delegatorCreationPolicy = WaitPolicy(200 milliseconds, NoRetry)))(
            initContext => new Initializer(initContext) {
              Thread.sleep(600)
              def onConnect = serverContext => new EchoHandler(serverContext)
            })
        serverProbe.watch(failedServer.server)
        serverProbe.expectTerminated(failedServer.server)
      }

    }

    "shutting down a system kills client connections" in {
      implicit val io = IOSystem("test-system", Some(2), MetricSystem.deadSystem)
      val server      = Server.basic("echo", TEST_PORT)(context => new EchoHandler(context))
      val probe       = TestProbe()
      probe watch server.server
      withServer(server) {
        val cio = IOSystem("client_io", Some(2), MetricSystem.deadSystem)
        val c   = TestClient(cio, TEST_PORT, connectRetry = NoRetry)
        Await.result(c.send(ByteString("HELLO")), 200.milliseconds) must equal(ByteString("HELLO"))
        io.shutdown()
        probe.expectTerminated(server.server)
        TestClient.waitForStatus(c, ConnectionStatus.NotConnected)
        cio.shutdown()
      }
    }

    "get server info" in {
      withServer(serverContext => new EchoHandler(serverContext)) { server =>
        server.server ! Server.GetInfo
        expectMsg(50.milliseconds, Server.ServerInfo(0, ServerStatus.Bound))
      }
    }

    "reject connection when maxed out" in {
      val settings = ServerSettings(
        port = TEST_PORT,
        maxConnections = 1
      )
      withIOSystem { implicit io =>
        val server = Server.basic("echo", settings)(serverContext => new EchoHandler(serverContext))
        withServer(server) {
          val c1 = TestClient(server.system, TEST_PORT)
          expectConnections(server, 1)
          val c2 = TestClient(server.system, TEST_PORT, false)
          expectConnections(server, 1)
        }
      }
    }

    "open up spot when connection closes" in {
      val settings = ServerSettings(
        port = TEST_PORT,
        maxConnections = 1
      )

      withIOSystem { implicit io =>
        val server = Server.basic("echo", settings)(serverContext => new EchoHandler(serverContext))
        withServer(server) {
          val c1 = TestClient(server.system, TEST_PORT)
          expectConnections(server, 1)
          c1.disconnect()
          TestUtil.expectServerConnections(server, 0)
          val c2 = TestClient(server.system, TEST_PORT, waitForConnected = true, connectRetry = NoRetry)
          TestUtil.expectServerConnections(server, 1)
        }
      }
    }

    "times out idle client connection" in {
      withIOSystem { implicit io =>
        val server = Server.basic("test", ServerSettings(port = TEST_PORT, maxIdleTime = 100.milliseconds))(
          serverContext => new EchoHandler(serverContext)
        )
        withServer(server) {
          val c = TestClient(server.system, TEST_PORT, connectRetry = NoRetry)
          expectConnections(server, 1)
          Thread.sleep(500)
          TestUtil.expectServerConnections(server, 0)
        }
      }
    }

    "stash delegator broadcast messages until workers report ready" in {
      val (sys, mprobe)     = FakeIOSystem.withManagerProbe()
      val server            = Server.basic("test", TEST_PORT)(serverContext => new EchoHandler(serverContext))(sys)
      val workerRouterProbe = TestProbe()
      server.initializerBroadcast("TEST")
      mprobe.expectMsgType[WorkerManager.RegisterServer](50.milliseconds)
      mprobe.expectNoMsg(100.milliseconds)
      server.server ! WorkerManager.WorkersReady(workerRouterProbe.ref)
      workerRouterProbe.expectMsgType[akka.routing.Broadcast](50.milliseconds)
      server.shutdown()
      sys.shutdown()
    }

    "properly registers when worker initially times out" in {
      //notice, this test failed due to a timeout beforet the fix
      class SleepyInitializer(server: ServerRef, worker: WorkerRef) extends Initializer(InitContext(server, worker)) {
        Thread.sleep(600)
        def acceptNewConnection                                            = None // >:(
        override def onConnect: (ServerContext) => ServerConnectionHandler = ???
      }
      withIOSystemAndServer((ic) => new SleepyInitializer(ic.server, ic.worker), waitTime = 10.seconds)(
        (io, sys) => ())
    }

    "switch to high water timeout when connection count passes the high water mark" in {
      //for now this test only checks to see that the server switched its status
      withIOSystem { implicit io =>
        val settings = ServerSettings(
          port = TEST_PORT,
          maxConnections = 4,
          lowWatermarkPercentage = 0.00,
          highWatermarkPercentage = 0.50,
          highWaterMaxIdleTime = 50.milliseconds,
          maxIdleTime = 1.hour
        )
        val server = Server.basic("test", settings)(serverContext => new EchoHandler(serverContext))
        withServer(server) {
          val idleConnection1 = TestClient(server.system, TEST_PORT, connectRetry = NoRetry)
          TestUtil.expectServerConnections(server, 1)
          val idleConnection2 = TestClient(server.system, TEST_PORT, connectRetry = NoRetry, waitForConnected = false)
          Thread.sleep(500) //have to wait a second since that's how often the check it done
          expectConnections(server, 0)
        }
      }
    }

    "initializer onShutdown is called when a worker shuts down" in {
      val alive = new AtomicInteger(0)
      class WhineyInitializer(initContext: InitContext) extends Initializer(initContext) {
        alive.incrementAndGet()
        def acceptNewConnection = None // >:(

        override def onConnect: (ServerContext) => ServerConnectionHandler = ???

        override def onShutdown() {
          alive.decrementAndGet()
        }
      }

      withIOSystemAndServer((initContext) => new WhineyInitializer(initContext), waitTime = 10.seconds) {
        (io, server) =>
          {
            alive.get() must equal(server.system.numWorkers)
          }
      }

      alive.get() must equal(0)

    }

    "attempt to re-register connection if refused by worker" in {
      val (sys, mprobe)     = FakeIOSystem.withManagerProbe()
      val server            = Server.basic("test", TEST_PORT)(serverContext => new EchoHandler(serverContext))(sys)
      val workerRouterProbe = TestProbe()
      mprobe.expectMsgType[WorkerManager.RegisterServer](50.milliseconds)
      server.server ! WorkerManager.WorkersReady(workerRouterProbe.ref)
      withIOSystem { implicit io =>
        val c = TestClient(io, TEST_PORT, connectRetry = NoRetry)
        (1 to Server.MaxConnectionRegisterAttempts).foreach { i =>
          val msg = workerRouterProbe.receiveOne(100.milliseconds).asInstanceOf[Worker.NewConnection]
          msg.attempt must equal(i)
          server.server ! Server.ConnectionRefused(msg.sc, msg.attempt)
        }
        TestClient.waitForStatus(c, ConnectionStatus.NotConnected)
      }
      server.shutdown()
      sys.shutdown()
    }
  }

  class TestInitializer(initContext: InitContext) extends Initializer(initContext) {
    override def onConnect: (ServerContext) => ServerConnectionHandler =
      (ctx) => new EchoHandler(ServerContext(ctx.server, ctx.context.worker.generateContext()))

    override def receive = {
      case a: ActorRef => a.!(())
    }
  }

  "initializer" must {
    "receive broadcast messages" in {
      withIOSystemAndServer(initContext => new TestInitializer(initContext)) { (io, server) =>
        val dprobe = TestProbe()
        server.server ! Server.InitializerBroadcast(dprobe.ref)
        dprobe.expectMsg(())
        dprobe.expectMsg(())
      }
    }
  }

}
