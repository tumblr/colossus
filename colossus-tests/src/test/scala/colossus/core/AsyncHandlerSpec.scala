package colossus


import testkit._
import core._

import akka.testkit.TestProbe

import scala.concurrent.duration._
import akka.util.ByteString

import ConnectionCommand._
import ConnectionEvent._
import AckLevel._

class AsyncHandlerSpec extends ColossusSpec {

  def withHandler(f: (MockWriteEndpoint, AsyncHandler, TestProbe, WorkerRef) => Any) {
    val (workerProbe, worker) = FakeIOSystem.fakeWorkerRef
    val handlerProbe = TestProbe()
    val handler = new AsyncHandler(handlerProbe.ref)
    val endpoint = new MockWriteEndpoint(10, workerProbe, Some(handler))
    f(endpoint, handler, handlerProbe, worker)
  }
    
  "AsyncHandler" must {
    "send Bound event" in {
      withHandler {case (endpoint, handler, probe, worker) => 
        handler.setBind(34, worker)
        probe.expectMsg(Bound(34))
      }
    }

    "send Connected event" in {
      withHandler{ case (endpoint, handler, probe, worker) => 
        handler.setBind(34, worker)
        probe.expectMsg(Bound(34))
        handler.connected(endpoint)
        probe.expectMsg(Connected)
      }
    }

    "send terminated event" in {
      withHandler{ case (endpoint, handler, probe, worker) => 
        handler.setBind(34, worker)
        probe.expectMsg(Bound(34))
        handler.connected(endpoint)
        probe.expectMsg(Connected)
        endpoint.disrupt()
        probe.expectMsg(ConnectionTerminated(DisconnectCause.Closed))
      }
    }

    "send write ack on success" in {
      withHandler{ case (endpoint, handler, probe, worker) => 
        handler.setBind(34, worker)
        probe.expectMsg(Bound(34))
        handler.connected(endpoint)
        probe.expectMsg(Connected)
        handler.receivedMessage(Write(ByteString("HELLO"), AckAll), probe.ref)
        probe.expectMsg(WriteAck(WriteStatus.Complete))
      }
    }

    "send partial ack and ReadyForData" in {
      withHandler{ case (endpoint, handler, probe, worker) => 
        handler.setBind(34, worker)
        probe.expectMsg(Bound(34))
        handler.connected(endpoint)
        probe.expectMsg(Connected)
        handler.receivedMessage(Write(ByteString("123456789012345"), AckAll), probe.ref)
        probe.expectMsg(WriteAck(WriteStatus.Partial))
        endpoint.clearBuffer()
        probe.expectMsg(ReadyForData)
      }

    }

    "send zero ack" in {
      withHandler{ case (endpoint, handler, probe, worker) => 
        handler.setBind(34, worker)
        probe.expectMsg(Bound(34))
        handler.connected(endpoint)
        probe.expectMsg(Connected)
        handler.receivedMessage(Write(ByteString("123456789012345"), AckAll), probe.ref)
        probe.expectMsg(WriteAck(WriteStatus.Partial))

        
        handler.receivedMessage(Write(ByteString("M"), AckAll), probe.ref)
        probe.expectMsg(WriteAck(WriteStatus.Zero))
        
        handler.receivedMessage(Write(ByteString("M"), AckFailure), probe.ref)
        probe.expectMsg(WriteAck(WriteStatus.Zero))
        handler.receivedMessage(Write(ByteString("M"), AckSuccess), probe.ref)
        probe.expectNoMsg(100.milliseconds)
        handler.receivedMessage(Write(ByteString("M"), AckNone), probe.ref)
        probe.expectNoMsg(100.milliseconds)

        endpoint.clearBuffer()
        probe.expectMsg(ReadyForData)
      }
    }
      



  }


}

