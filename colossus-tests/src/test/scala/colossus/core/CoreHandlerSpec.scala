package colossus.core

import colossus.testkit._

import scala.concurrent.duration._

class TestHandler extends BasicCoreHandler {

  var shutdownCalled = false

  override def shutdown() {
    shutdownCalled =true
    super.shutdown()
  }

}

class CoreHandlerSpec extends ColossusSpec {

  def setup(): (Connection with MockConnection, TestHandler) = {
    val handler = new TestHandler
    val con = MockConnection.server(handler)
    handler.connected(con)
    (con, handler)

  }

  "Core Handler" must {
    
    "set connectionStatus to Connected" in {
      val handler = new BasicCoreHandler
      val con = MockConnection.server(handler)
      handler.connectionState must equal(ConnectionState.NotConnected)
      handler.connected(con)
      handler.connectionState must equal(ConnectionState.Connected(con))
    }

    "set connectionState to NotConnected on disrupted connection" in {
      val (con, handler) = setup()
      con.disrupt()
      handler.connectionState must equal(ConnectionState.NotConnected)
    }

    "disconnect" in {
      val (con, handler) = setup()
      handler.disconnect()
      handler.shutdownCalled must equal(false)
      con.workerProbe.expectMsg(100.milliseconds, WorkerCommand.Disconnect(con.id))
    }

    "graceful disconnect" in {
      val (con, handler) = setup()
      handler.gracefulDisconnect()
      handler.shutdownCalled must equal(true)
      con.workerProbe.expectMsg(100.milliseconds, WorkerCommand.Disconnect(con.id))
    }

    "become" in {
      val f = new BasicCoreHandler
      val (con, handler) = setup()
      handler.become(f)
      handler.shutdownCalled must equal(true)
      val m = con.workerProbe.receiveOne(100.milliseconds).asInstanceOf[WorkerCommand.SwapHandler]
      m.id must equal(con.id)
      m.newWorkerItem() must equal(f)
    }

    "connection state set to ShuttingDown while shutting down" in {
      val (con, handler) = setup()
      handler.shutdownRequest()
      handler.connectionState must equal(ConnectionState.ShuttingDown(con))
    }

    "not call shutdown more than once" in {
      val (con, handler) = setup()
      handler.shutdownRequest()
      handler.shutdownCalled must equal(true)
      handler.shutdownCalled = false
      handler.shutdownRequest()
      handler.shutdownCalled must equal(false)
    }
      

      

  }

}
