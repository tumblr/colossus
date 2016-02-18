package colossus.core

import colossus.testkit._

import scala.concurrent.duration._

class TestHandler(ctx: Context) extends BasicCoreHandler(ctx) {

  var shutdownCalled = false

  override def shutdown() {
    shutdownCalled =true
    super.shutdown()
  }

}

class CoreHandlerSpec extends ColossusSpec {

  def setup(): TypedMockConnection[TestHandler] = {
    val con = MockConnection.server(new TestHandler(_))
    con.handler.connected(con)
    con
  }

  "Core Handler" must {
    
    "set connectionStatus to Connected" in {
      val con = MockConnection.server(new BasicCoreHandler(_))
      con.typedHandler.connectionState must equal(ConnectionState.NotConnected)
      con.typedHandler.connected(con)
      con.typedHandler.connectionState must equal(ConnectionState.Connected(con))
    }

    "set connectionState to NotConnected on disrupted connection" in {
      val con = setup()
      con.disrupt()
      con.typedHandler.connectionState must equal(ConnectionState.NotConnected)
    }

    "disconnect" in {
      val con = setup()
      con.typedHandler.disconnect()
      con.typedHandler.shutdownCalled must equal(false)
      con.workerProbe.expectMsg(100.milliseconds, WorkerCommand.Disconnect(con.id))
    }

    "graceful disconnect" in {
      val con = setup()
      con.typedHandler.gracefulDisconnect()
      con.typedHandler.shutdownCalled must equal(true)
      con.workerProbe.expectMsg(100.milliseconds, WorkerCommand.Disconnect(con.id))
    }

    "become" in {
      val con = setup()
      val f = new BasicCoreHandler(con.typedHandler.context)
      con.typedHandler.become(_ => f)
      con.typedHandler.shutdownCalled must equal(true)
      val m = con.workerProbe.receiveOne(100.milliseconds).asInstanceOf[WorkerCommand.SwapHandler]
      m.newHandler must equal(f)
    }

    "connection state set to ShuttingDown while shutting down" in {
      val con = setup()
      con.typedHandler.shutdownRequest()
      con.typedHandler.connectionState must equal(ConnectionState.ShuttingDown(con))
    }

    "not call shutdown more than once" in {
      val con = setup()
      con.typedHandler.shutdownRequest()
      con.typedHandler.shutdownCalled must equal(true)
      con.typedHandler.shutdownCalled = false
      con.typedHandler.shutdownRequest()
      con.typedHandler.shutdownCalled must equal(false)
    }
      

      

  }

}
