package colossus
package controller

import akka.util.ByteString
import colossus.core._
import colossus.parsing.DataSize._
import colossus.testkit._

import scala.concurrent.duration._
import scala.util.Success

class InputControllerSpec extends ColossusSpec with CallbackMatchers{

  import TestController._

  "Input Controller" must {

    "receive a message" in {
      val input = ByteString("hello")
      val con = static()
      con.typedHandler.receivedData(DataBuffer(input))
      con.typedHandler.received.size must equal(1)
      con.typedHandler.received.head must equal(input)
    }

    "decode a stream message" in {
      val expected = ByteString("Hello world!")
      val request = ByteString(expected.size.toString) ++ ByteString("\r\n") ++ expected
      var result = ByteString("na")
      val con = stream()
      con.typedHandler.receivedData(DataBuffer(request))
      con.typedHandler.received.head.source.pullCB().execute{
        case Success(Some(bytes)) => result = ByteString(bytes.takeAll)
        case _ => {}
      }
      result must equal(expected)
    }

    "disconnect from read events when pipe fills up" in {
      val con = stream()
      con.readsEnabled must equal(true)
      con.typedHandler.receivedData(DataBuffer(ByteString("4\r\n")))
      val m = con.typedHandler.received.head
      con.readsEnabled must equal(true)
      con.typedHandler.receivedData(DataBuffer(ByteString("a")))
      con.readsEnabled must equal(false)

      var executed = false
      m.source.fold(0){(a, b) => b + a.takeAll.length}.execute{
        case Success(4) => {executed = true}
        case other => {
          throw new Exception(s"bad result $other")
        }
      }
      //we have begun execution of the fold, which should drain the pipe, but
      //execution should not yet be complete
      executed must equal(false)
      con.readsEnabled must equal(true)
      con.typedHandler.receivedData(DataBuffer(ByteString("b")))
      con.typedHandler.receivedData(DataBuffer(ByteString("c")))
      con.typedHandler.receivedData(DataBuffer(ByteString("d")))
      //now it should be done
      executed must equal(true)
    }

    "reject data above the size limit" in {
      val input = ByteString("hello")
      val config = ControllerConfig(4, 50.milliseconds, 2.bytes)
      val con = static(config)
      con.typedHandler.receivedData(DataBuffer(input))
      con.typedHandler.received.isEmpty must equal(true)
      con.readsEnabled must equal(false)
    }
    /*

    "stream is terminated when connection disrupted" in {
      var source: Option[Source[DataBuffer]] = None
      val (endpoint, con) = createController({input =>
        source = Some(input.source)
      })
      con.receivedData(DataBuffer(ByteString("4\r\n")))
      source.isDefined must equal(true)
      val s = source.get
      endpoint.disrupt()
      var failed = false
      var wrong = false
      s.pullCB().execute {
        case Failure(t: PipeTerminatedException) => failed = true
        case other => {
          wrong = true
          throw new Exception(s"Invalid result $other")
        }
      }
      wrong must equal(false)
      failed must equal(true)
    }

    "input stream allowed to complete during graceful disconnect" in {
      var source: Option[Source[DataBuffer]] = None
      val (endpoint, con) = createController({input =>
        source = Some(input.source)
      })
      endpoint.readsEnabled must equal(true)
      con.receivedData(DataBuffer(ByteString("4\r\nab")))
      endpoint.readsEnabled must equal(false)
      var total = 0
      source.get.fold(0){(buf, len) => len + buf.size}.execute{
        case Success(l) => total = l
        case Failure(err) => throw err
      }
      endpoint.readsEnabled must equal(true)
      con.disconnect()
      //input stream is not done, so reads should still be enabled
      endpoint.readsEnabled must equal(true)
      con.receivedData(DataBuffer(ByteString("cd")))
      //the stream should have finished, so now reads should be disabled
      endpoint.readsEnabled must equal(false)
      total must equal(4)

    }
    */



  }




}

