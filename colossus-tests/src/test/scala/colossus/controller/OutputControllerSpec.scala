package colossus
package controller

import core._
import testkit._
import akka.util.ByteString

import scala.concurrent.duration._



class OutputControllerSpec extends ColossusSpec {

  import TestController._

  "OutputController" must {
    "push a message" in {
      val endpoint = static()
      val message = ByteString("Hello World!")
      val p = endpoint.typedHandler.pPush(message)
      p.isSet must equal(false)
      endpoint.iterate()
      endpoint.expectOneWrite(message)
      p.isSuccess must equal(true)

    }
    "push multiple messages" in {
      val endpoint = static()
      val data = ByteString("Hello World!")
      val p1 = endpoint.typedHandler.pPush(data)
      val p2 = endpoint.typedHandler.pPush(data)
      endpoint.iterate()
      endpoint.expectOneWrite(data ++ data)
      p1.expectSuccess()
      p2.expectSuccess()

    }

    "push a streaming message" in {
      val endpoint = stream()
      val pieces = List("a", "b", "c").map{s => ByteString(s)}
      val gen = new IteratorGenerator(pieces.map{DataBuffer(_)}.toIterator)
      val message = TestOutput(gen)
      val p = endpoint.typedHandler.pPush(message)
      p.expectNoSet
      endpoint.iterate() //this first iteration simply encodes the message and starts the pull
      p.expectNoSet
      endpoint.iterate() //this one should do the writes
      endpoint.expectOneWrite(ByteString("abc"))
      p.expectSuccess
    }


    "respect buffer soft overflow" in {
      val endpoint = static()
      val over = ByteString(List.fill(110)("a").mkString)
      val next = ByteString("hey")
      val p1 = endpoint.typedHandler.pPush(over)
      val p2 = endpoint.typedHandler.pPush(next)
      endpoint.iterate()
      p1.expectSuccess()
      p2.expectNoSet
      endpoint.iterate()
      p2.expectSuccess()
    }

    "respect pausing writes while writing" in {
      val endpoint = static()
      val data = ByteString("hello")
      endpoint.typedHandler.testPush(data){ case _ => endpoint.typedHandler.testPause() }
      val p = endpoint.typedHandler.pPush(data)
      endpoint.iterate()
      endpoint.expectOneWrite(data)
      p.expectNoSet()
      endpoint.typedHandler.testResume()
      endpoint.iterate()
      p.expectSuccess()
    }

    "not request a write when writes are paused" in {
      val endpoint = static()
      val data = ByteString("hello")
      endpoint.writeReadyEnabled must equal(false)
      endpoint.typedHandler.testPause()
      val p = endpoint.typedHandler.pPush(data)
      endpoint.writeReadyEnabled must equal(false)
    }



    "drain output buffer on disconnect" in {
      val endpoint = static()
      val over = ByteString(List.fill(110)("a").mkString)
      val next = ByteString("hey")
      val p1 = endpoint.typedHandler.pPush(over)
      val p2 = endpoint.typedHandler.pPush(next)
      endpoint.typedHandler.disconnect()
      endpoint.workerProbe.expectNoMsg(100.milliseconds)
      endpoint.iterate()
      p1.expectSuccess()
      p2.expectNoSet
      endpoint.iterate()
      p2.expectSuccess()
      //final iterate is needed to do the disconnect check
      endpoint.iterate()
      endpoint.workerProbe.expectMsg(100.milliseconds, WorkerCommand.Disconnect(endpoint.id))
    }

    "timeout queued messages that haven't been sent" in {
      val endpoint = static()
      val p = endpoint.typedHandler.pPush(ByteString("wat"))
      Thread.sleep(300)
      endpoint.typedHandler.idleCheck(1.millisecond)
      p.expectCancelled()
    }


    "fail pending messages on connectionClosed while gracefully disconnecting"  in {
      val endpoint = static()
      val p = endpoint.typedHandler.pPush(ByteString("hello"))
      endpoint.typedHandler.disconnect()
      endpoint.disconnectCalled must equal(false)
      p.expectNoSet()
      endpoint.disrupt()
      p.expectCancelled()
    }

    "not fail pending messages when connection disrupted" in {
      val endpoint = static()
      val p = endpoint.typedHandler.pPush(ByteString("hello"))
      endpoint.disrupt()
      p.expectNoSet()
    }



  }



}
