package colossus
package controller

import akka.util.ByteString
import colossus.testkit._
import core._

import scala.util.{Try, Failure, Success}

class InputControllerSpec extends ColossusSpec with CallbackMatchers{
  
  import TestController.createController

  "Input Controller" must {

    "decode a stream message" in {
      val expected = ByteString("Hello world!")
      val request = ByteString(expected.size.toString) ++ ByteString("\r\n") ++ expected
      var called = false
      val (endpoint, con) = createController({input => 
        input.source.pullCB().execute{
          case Success(Some(data)) => {
            ByteString(data.takeAll) must equal(expected)
            called = true
          }
          case _ => throw new Exception("wrong result")
        }
      })
      called must equal(false)
      con.receivedData(DataBuffer(request))
      called must equal(true)
    }

    "disconnect from read events when pipe fills up" in {
      var source: Option[Source[DataBuffer]] = None
      val (endpoint, con) = createController({input => 
        source = Some(input.source)
      })
      endpoint.readsEnabled must equal(true)
      con.receivedData(DataBuffer(ByteString("4\r\n")))
      source.isDefined must equal(true)
      con.receivedData(DataBuffer(ByteString("a")))
      con.receivedData(DataBuffer(ByteString("b")))
      endpoint.readsEnabled must equal(true)
      //this last piece should fill up the pipe
      con.receivedData(DataBuffer(ByteString("c")))
      endpoint.readsEnabled must equal(false)

      var executed = false
      source.get.fold(0){(a, b) => b + a.takeAll.length}.execute{
        case Success(4) => {executed = true}
        case other => {
          throw new Exception(s"bad result $other")
        }
      }
      //we have begun execution of the fold, which should drain the pipe, but
      //execution should not yet be complete
      executed must equal(false)
      endpoint.readsEnabled must equal(true)
      con.receivedData(DataBuffer(ByteString("d")))
      //now it should be done
      executed must equal(true)
    }

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
      source.get.pullCB().map{buf => ByteString(buf.get.takeAll)} must evaluateTo{b: ByteString => 
        b must equal(ByteString("ab"))
      }
      con.testGracefulDisconnect()
      //input stream is not done, so reads should still be enabled
      endpoint.readsEnabled must equal(true)
      con.receivedData(DataBuffer(ByteString("cd")))
      source.get.pullCB().map{buf => ByteString(buf.get.takeAll)} must evaluateTo{b: ByteString => 
        b must equal(ByteString("cd"))
      }
      //the stream should have finished, so now reads should be disabled
      endpoint.readsEnabled must equal(false)

    }
        
      

  }




}

