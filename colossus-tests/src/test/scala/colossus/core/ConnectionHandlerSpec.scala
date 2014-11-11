package colossus

import testkit._
import core._

import akka.actor._
import akka.testkit.TestProbe

import scala.concurrent.duration._
import akka.util.ByteString

import ConnectionCommand._
import ConnectionEvent._

class Handler(listener: ActorRef) extends Actor {
  def receive = {
    case m => listener ! m
  }
}
  
class AsyncDelegator(props: Props, server: ServerRef, worker: WorkerRef)(implicit factory: ActorRefFactory) extends Delegator(server, worker) {
  implicit val w = worker.worker
  def acceptNewConnection = Some(AsyncHandler(factory.actorOf(props), worker))
}
object AsyncDelegator {
  def factorize(props: Props)(implicit system: ActorSystem): Delegator.Factory = {
    (server, worker) => new AsyncDelegator(props, server, worker)
  }
}

class ConnectionHandlerSpec extends ColossusSpec {

  def createHandlerServer(): (ServerRef, TestConnection, TestProbe) = {
    val probe = TestProbe()
    val props = Props(classOf[Handler], probe.ref)
    val server = createServer(AsyncDelegator.factorize(props))
    val c = new TestConnection(TEST_PORT)
    probe.expectMsgType[ConnectionEvent.Connected]
    (server, c, probe)
  }

  "Async Server Handler" must {
    "receive connected event" in {
      val (server, con, probe) = createHandlerServer()
      end(server)
    }
    "receive connection lost event" in {
      val (server, con, probe) = createHandlerServer()
      con.close()
      probe.expectMsgType[ConnectionEvent.ConnectionTerminated]
      end(server)
    }
    "receive data event" in {
      val (server, con, probe) = createHandlerServer()
      con.write(ByteString("HELLO WORLD"))
      probe.expectMsgPF(100.milliseconds){
        case ConnectionEvent.ReceivedData(data) if (data == ByteString("HELLO WORLD")) => true
      }
      end(server)
    }
    "send data back to client" in {
      val (server, con, probe) = createHandlerServer()
      con.write(ByteString("ECHO"))
      probe.expectMsg(ReceivedData(ByteString("ECHO")))
      end(server)
    }

  }


}

