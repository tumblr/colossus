package colossus.core

import colossus.testkit._

import scala.concurrent.duration._

import ConnectionState._
import colossus.NoopHandler

//callSuperShutdown is only used in the last test
class TestHandler(ctx: ServerContext, callSuperShutdown: Boolean) extends NoopHandler(ctx.context) {

  var shutdownCalled = false

  override def onShutdown() {
    shutdownCalled = true
    if (callSuperShutdown) super.onShutdown()
  }

}

class CoreHandlerSpec extends ColossusSpec {

  def setup(callSuperShutdown: Boolean = true): TypedMockConnection[TestHandler] = {
    val con = MockConnection.server(serverContext => new TestHandler(serverContext, callSuperShutdown))
    con.handler.connected(con)
    con
  }

  "Core Handler" must {

    "set connectionStatus to Connected" in {
      val con = MockConnection.server(srv => new NoopHandler(srv.context))
      con.typedHandler.connectionState must equal(NotConnected)
      con.typedHandler.connected(con)
      con.typedHandler.connectionState must equal(Connected(con))
    }

    "set connectionState to NotConnected on disrupted connection" in {
      val con = setup()
      con.disrupt()
      con.typedHandler.connectionState must equal(NotConnected)
    }

    "forceDisconnect" in {
      val con = setup()
      con.typedHandler.forceDisconnect()
      con.typedHandler.shutdownCalled must equal(false)
      con.workerProbe.expectMsg(100.milliseconds, WorkerCommand.Disconnect(con.id))
    }

    "disconnect" in {
      val con = setup()
      con.typedHandler.disconnect()
      con.typedHandler.shutdownCalled must equal(true)
      con.workerProbe.expectMsg(100.milliseconds, WorkerCommand.Disconnect(con.id))
    }

    "kill" in {
      val con = setup()
      con.typedHandler.kill(new Exception("foo"))
      con.typedHandler.shutdownCalled must equal(false)
      con.workerProbe.receiveOne(100.milliseconds) match {
        case WorkerCommand.Kill(id, cause) => {
          id mustBe con.id
          cause mustBe a[DisconnectCause.Error]
        }
        case _ => throw new Exception("WRONG")
      }
    }

    "kill does nothing if the connection isn't connected" in {
      val con = MockConnection.server(serverContext => new TestHandler(serverContext, true))
      con.typedHandler.kill(new Exception("foo"))
      con.workerProbe.expectNoMsg(100.milliseconds)
    }

    "become" in {
      val con = setup()
      val f   = new NoopHandler(con.typedHandler.context)
      con.typedHandler.become(() => f)
      con.typedHandler.shutdownCalled must equal(true)
      val m = con.workerProbe.receiveOne(100.milliseconds).asInstanceOf[WorkerCommand.SwapHandler]
      m.newHandler must equal(f)
    }

    "connection state set from Connected to ShuttingDown while shutting down" in {
      val con = setup()
      con.typedHandler.shutdownRequest()
      con.typedHandler.connectionState must equal(ShuttingDown(con))
    }

    "connection state stays in NotConnected while shutting down" in {
      val con = MockConnection.server(serverContext => new TestHandler(serverContext, true))
      con.typedHandler.connectionState must equal(NotConnected)
      con.typedHandler.shutdownRequest()
      con.typedHandler.connectionState must equal(NotConnected)
    }

    "not call shutdown when disconnect is called while in the shuttingdown state" in {
      val con = setup(false)
      con.typedHandler.shutdownRequest()
      con.typedHandler.shutdownCalled must equal(true)
      con.typedHandler.shutdownCalled = false
      con.typedHandler.shutdownRequest()
      con.typedHandler.shutdownCalled must equal(false)
    }

  }

}
