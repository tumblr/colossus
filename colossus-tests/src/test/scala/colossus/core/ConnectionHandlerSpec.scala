package colossus

import colossus.testkit.ColossusSpec
import core._
import service.Callback
import Callback.Implicits._

import akka.actor._
import akka.testkit.TestProbe

import java.net.InetSocketAddress
import scala.concurrent.duration._

import RawProtocol._

class ConnectionHandlerSpec extends ColossusSpec {

  "Server Connection Handler" must {
    "bind to worker on creation" in {
      val probe = TestProbe()
      class MyHandler(context: ServerContext) extends NoopHandler(context) {
        override def onBind() {
          probe.ref ! "BOUND"
        }
      }
      withServer(context => new MyHandler(context)) { server =>
        val c = TestClient(server.system, TEST_PORT)
        probe.expectMsg(100.milliseconds, "BOUND")
      }
    }

    "unbind on disconnect" in {
      val probe = TestProbe()
      class MyHandler(context: ServerContext) extends NoopHandler(context) {
        override def onUnbind() {
          probe.ref ! "UNBOUND"
        }
      }
      withServer(context => new MyHandler(context)) { server =>
        val c = TestClient(server.system, TEST_PORT, connectRetry = NoRetry)
        c.disconnect()
        TestClient.waitForStatus(c, ConnectionStatus.NotConnected)
        probe.expectMsg(500.milliseconds, "UNBOUND")
      }
    }
  }

  "Client Connection Handler" must {

    class MyHandler(context: Context,
                    probe: ActorRef,
                    sendBind: Boolean,
                    sendUnbind: Boolean,
                    disconnect: Boolean = false)
        extends NoopHandler(context)
        with ClientConnectionHandler {
      override def onBind() {
        if (sendBind) probe ! "BOUND"
      }
      override def onUnbind() {
        println("UNBOUND")
        if (sendUnbind) probe ! "UNBOUND"
      }

      override def connected(endpoint: WriteEndpoint) {
        println("CONNECTED")
        if (disconnect) endpoint.disconnect()
      }

      override def connectionTerminated(cause: DisconnectCause) {
        println(s"TERMINATED: $cause")
      }

    }

    "bind to worker" in {
      val probe = TestProbe()
      withIOSystem { implicit io =>
        //obvious this will fail to connect, but we don't care here
        io ! IOCommand.BindAndConnectWorkerItem(new InetSocketAddress("localhost", TEST_PORT),
                                                context => new MyHandler(context, probe.ref, true, false))
        probe.expectMsg(100.milliseconds, "BOUND")
      }
    }

    "automatically unbind on manual disconnect" in {
      val probe = TestProbe()
      class MyHandler(context: Context) extends NoopHandler(context) {
        override def onUnbind() {
          probe.ref ! "UNBOUND"
        }
        override def connected(endpoint: WriteEndpoint) {
          endpoint.disconnect()
        }
      }
      withIOSystem { implicit io =>
        withServer(RawServer.basic("test", TEST_PORT) { case x => x }) {
          io ! IOCommand.BindAndConnectWorkerItem(new InetSocketAddress("localhost", TEST_PORT), c => new MyHandler(c))
          probe.expectMsg(250.milliseconds, "UNBOUND")
        }
      }
    }

    "automatically unbind on disrupted connection" in {
      val probe = TestProbe()
      withIOSystem { implicit io =>
        val server = RawServer.basic("test", TEST_PORT) { case x => x }
        withServer(server) {
          io ! IOCommand.BindAndConnectWorkerItem(
            new InetSocketAddress("localhost", TEST_PORT),
            c => new MyHandler(c, probe.ref, true, true)
          )
          probe.expectMsg(500.milliseconds, "BOUND")
        }
        end(server)
        println("server ENDED")
        probe.expectMsg(5000.milliseconds, "UNBOUND")
      }

    }

    "automatically unbind on failure to connect" taggedAs (org.scalatest.Tag("test")) in {
      val probe = TestProbe()
      withIOSystem { implicit io =>
        io ! IOCommand.BindAndConnectWorkerItem(
          new InetSocketAddress("localhost", TEST_PORT),
          c => new MyHandler(c, probe.ref, true, true)
        )
        probe.expectMsg(250.milliseconds, "BOUND")
        probe.expectMsg(10.seconds, "UNBOUND")
      }
    }

    "NOT automatically unbind with ManualUnbindHandler mixin on disrupted connection" in {
      val probe = TestProbe()
      class MyHandler(context: Context) extends NoopHandler(context) with ManualUnbindHandler {
        override def onUnbind() {
          probe.ref ! "UNBOUND"
        }
      }
      withIOSystem { implicit io =>
        withServer(RawServer.basic("test", TEST_PORT) { case x => x }) {
          io ! IOCommand.BindAndConnectWorkerItem(new InetSocketAddress("localhost", TEST_PORT), c => new MyHandler(c))
          probe.expectNoMsg(200.milliseconds)
        }
        probe.expectNoMsg(200.milliseconds)
      }
    }

    "NOT automatically unbind on failed connection with ManualUnbindHandler" in {
      val probe = TestProbe()
      class MyHandler(context: Context) extends NoopHandler(context) with ManualUnbindHandler {
        override def onUnbind() {
          probe.ref ! "UNBOUND"
        }
      }
      withIOSystem { implicit io =>
        io ! IOCommand.BindAndConnectWorkerItem(new InetSocketAddress("localhost", TEST_PORT), c => new MyHandler(c))
        probe.expectNoMsg(200.milliseconds)
      }

    }
  }

}
