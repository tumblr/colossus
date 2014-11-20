package colossus

import testkit._
import core._

import akka.actor._
import akka.agent._
import akka.testkit.TestProbe

import scala.concurrent.duration._
import akka.util.ByteString

import org.scalatest.Tag

object NewTest extends Tag("NewTest")

class ServerSpec extends ColossusSpec {

  def expectConnections(server: ServerRef, num: Int) {
    server.server ! Server.GetInfo
    expectMsg(50.milliseconds, Server.ServerInfo(num, ServerStatus.Bound))
 }

 val EchoServerConfig = ServerConfig(
    name = "test-server",
    settings = ServerSettings(
      port = TEST_PORT
    ),
    delegatorFactory = Delegator.basic(() => new EchoHandler)
  )

  "IOSystem" must {
    "startup and shutdown" in {
      val io = IOSystem("test", 2)
      io ! WorkerManager.ReadyCheck
      expectMsg(100.milliseconds, WorkerManager.WorkersNotReady)
      Thread.sleep(100) //if it takes longer than this we're in trouble
      io ! WorkerManager.ReadyCheck
      val probe = TestProbe()
      probe watch io.workerManager
      expectMsgClass(50.milliseconds, classOf[WorkerManager.WorkersReady])
      io.shutdown()
      probe.expectTerminated(io.workerManager)
    }
  }

  "Server" must {
    "attach to a system and start" in {
      withIOSystem { implicit io =>
        val server = Server.basic("echo", TEST_PORT, () => new EchoHandler)
        waitForServer(server)
        val c = new TestConnection(TEST_PORT)
        val data = ByteString("hello world!")
        c.write(data)
        c.read() must equal(data)
      }
    }

    "shutting down system shuts down attached servers" in {
      implicit val io = IOSystem("test", 2)
      val probe = TestProbe()
      probe watch io.workerManager
      val server = Server.basic("echo", TEST_PORT, () => new EchoHandler)
      val probe2 = TestProbe()
      probe2 watch server.server
      Thread.sleep(100)
      io.shutdown()
      probe2.expectTerminated(server.server)
      probe.expectTerminated(io.workerManager)
    }

    "indicates when it cannot bind to a port when a duration is supplied" in {
      withIOSystem { implicit io =>
        val existingServer = Server.basic("echo3", TEST_PORT, () => new EchoHandler)
        waitForServer(existingServer)
        val settings = ServerSettings(port = TEST_PORT, bindingAttemptDuration = Some(PollingDuration(50 milliseconds, Some(1L))))
        val cfg = ServerConfig("echo2", Delegator.basic(() => new EchoHandler), settings)
        val p = TestProbe()
        val clashingServer: ServerRef = Server(cfg)
        p.watch(clashingServer.server)
        p.expectTerminated(clashingServer.server)
      }
    }

    "shutting down a system kills client connections" ignore {
      implicit val io = IOSystem("test", 2)
      val probe = TestProbe()
      val server = Server.basic("echo", TEST_PORT, () => new EchoHandler)
      probe watch server.server
      Thread.sleep(100)
      val c = new TestConnection(TEST_PORT)
      c.socket.isConnected must equal(true)
      c.write(ByteString("HELLO"))
      c.read() must equal(ByteString("HELLO"))
      io.shutdown()
      probe.expectTerminated(server.server)
      c.isClosed must equal (true)
    }

    "get server info" in {
      val server = createServer(Delegator.basic(() => new EchoHandler))
      server.server ! Server.GetInfo
      expectMsg(50.milliseconds, Server.ServerInfo(0, ServerStatus.Bound))
      end(server)
    }
      

    "reject connection when maxed out" ignore {
      val settings = ServerSettings(
        port = TEST_PORT,
        maxConnections = 1
      )
      val server = createServer(Delegator.basic(() => new EchoHandler), Some(settings))
      val c1 = new TestConnection(TEST_PORT)
      c1.socket.isConnected must equal(true)
      expectConnections(server, 1)
      val c2 = new TestConnection(TEST_PORT)
      c2.socket.isConnected must equal(true)
      expectConnections(server, 1)
      c2.write(ByteString("testing"))
      c2.isClosed must equal (true)
      end(server)
    }

    "open up spot when connection closes" ignore {
      val settings = ServerSettings(
        port = TEST_PORT,
        maxConnections = 1
      )
      val server = createServer(Delegator.basic(() => new EchoHandler), Some(settings))
      val c1 = new TestConnection(TEST_PORT)
      c1.socket.isConnected must equal(true)
      expectConnections(server, 1)
      c1.write(ByteString("testing"))
      c1.read() must equal(ByteString("testing"))
      c1.close()
      Thread.sleep(50) 
      expectConnections(server, 0)
      val c2 = new TestConnection(TEST_PORT)
      c2.socket.isConnected must equal(true)
      c2.write(ByteString("testing"))
      Thread.sleep(50)
      c2.read() must equal(ByteString("testing"))
      end(server)

    }

    "close connection when worker rejects" ignore {
      class AngryDelegator(server: ServerRef, worker: WorkerRef) extends Delegator(server, worker) {
        def acceptNewConnection = None // >:(
      }
      val server = createServer((s,w) => new AngryDelegator(s,w))
      val c1 = new TestConnection(TEST_PORT)

      c1.write(ByteString("testing"))
      Thread.sleep(100)
      expectConnections(server, 0)
      Thread.sleep(100)
      //intercept[java.net.SocketException] {
        c1.read() must equal(ByteString())
      //}
      end(server)

    }

    "times out idle client connection" ignore {
      withIOSystem { implicit io =>
        val probe = TestProbe()
        val config = ServerConfig(
          name = "test",
          settings = ServerSettings(
            port = TEST_PORT,
            maxIdleTime = 100.milliseconds
          ),
          delegatorFactory = Delegator.basic(() => new EchoHandler)
        )
        val server = Server(config)
        probe watch server.server
        Thread.sleep(30)
        val c = new TestConnection(TEST_PORT)
        c.socket.isConnected must equal(true)
        expectConnections(server, 1)
        Thread.sleep(1000)
        expectConnections(server, 0)
        c.isClosed must equal(true)
      }
    }

    "stash delegator broadcast messages until workers report ready" ignore {
      val (sys, mprobe) = FakeIOSystem.withManagerProbe()
      val config = EchoServerConfig
      val server = Server(config)(sys)
      val workerRouterProbe = TestProbe()
      server.delegatorBroadcast("TEST")
      mprobe.expectMsgType[WorkerManager.RegisterServer](50.milliseconds)
      mprobe.expectNoMsg(100.milliseconds)
      server.server ! WorkerManager.WorkersReady(workerRouterProbe.ref)
      workerRouterProbe.expectMsgType[akka.routing.Broadcast](50.milliseconds)
      server.server ! PoisonPill
      end(server)
    }

    "properly registers when worker initially times out" in {
      //notice, this test failed due to a timeout beforet the fix
      class SleepyDelegator(server: ServerRef, worker: WorkerRef) extends Delegator(server, worker) {
        Thread.sleep(600)
        def acceptNewConnection = None // >:(
      }
      end(createServer((s,w) => new SleepyDelegator(s,w), waitTime = 10.seconds))

    }

    //NOTICE - this test flaps sometimes , consider rewriting
    "switch to high water timeout when connection count passes the high water mark" ignore {
      withIOSystem { implicit io =>
        val config = ServerConfig(
          name = "highWaterTest",
          settings = ServerSettings(
            port = TEST_PORT,
            maxConnections = 10,
            lowWatermarkPercentage = 0.60,
            highWatermarkPercentage = 0.80,
            highWaterMaxIdleTime = 50.milliseconds
          ),
          delegatorFactory = Delegator.basic(() => new EchoHandler)
        )
        val server = Server(config)
        waitForServer(server)
        val idleConnections = for{i <- 1 to 5} yield new TestConnection(TEST_PORT)
        Thread.sleep(100)
        expectConnections(server, 5)
         //these should push us over the edge of the high water mark
         val chattyConnections = for{i <- 1 to 4} yield chattyConnection(TEST_PORT)
        chattyConnections.foreach(_._1.start())
         //we should now be right above the watermark //the first 5 should be culled
        Thread.sleep(1000)
        expectConnections(server, 4)
        idleConnections.foreach { _.isClosed must equal(true)}
        chattyConnections.foreach { case (t, c) =>
          t.isAlive must equal(true)
          t.isInterrupted must equal(false)
          c.running = false
        }
        Thread.sleep(230)
        chattyConnections.foreach{case (t, _) => t.join()}
      }
    }

    "delegator onShutdown is called when a worker shuts down" in {
      import scala.concurrent.ExecutionContext.Implicits.global
      val alive = Agent(0)
      class WhineyDelegator(server: ServerRef, worker: WorkerRef) extends Delegator(server, worker) {
        alive send {_ + 1}
        def acceptNewConnection = None // >:(

        override def onShutdown() {
          alive send {_ - 1}
        }
      }
      val server = createServer((s,w) => new WhineyDelegator(s,w), waitTime = 10.seconds)
      waitForServer(server)
      alive() must equal(server.system.config.numWorkers)
      end(server)
      alive() must equal(0)

    }
  }

  def chattyConnection(port : Int) : (Thread, RunnableTestConnection) = {
    val runnable = new RunnableTestConnection(true, new TestConnection(port))
    val t = new Thread(runnable)
    (t, runnable)
  }

  class RunnableTestConnection(var running : Boolean = true, val conn : TestConnection) extends Runnable
  {
    override def run() {
      while(running) {
        conn.write(ByteString("hi"))
        Thread.sleep(50)
      }
    }
  }

  class TestDelegator(server: ServerRef, worker: WorkerRef) extends Delegator(server, worker) {
    def acceptNewConnection = Some(new EchoHandler)
    override def handleMessage = {
      case a: ActorRef => a.!(())
    }    
  }

  "delegator" must {
    "receive broadcast messages" in {
      withIOSystem { implicit io =>
        val server = createServer((s,w) => new TestDelegator(s,w))
        val dprobe = TestProbe()
        server.server ! Server.DelegatorBroadcast(dprobe.ref)
        dprobe.expectMsg(())
        dprobe.expectMsg(())
      }
    }
  }

}
