package colossus
package controller

import scala.util.{Try, Success, Failure}
import core._
import service.Codec
import org.scalatest._
import akka.util.ByteString
import colossus.testkit._




class InputControllerSpec extends ColossusSpec {
  
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
      con.receivedData(DataBuffer(ByteString("c")))
      endpoint.readsEnabled must equal(false)

      var executed = false
      source.get.fold(0){(a, b) => b + a.takeAll.length}.execute{
        case Success(4) => {executed = true}
        case other => {
          throw new Exception(s"bad result $other")
        }
      }
      executed must equal(false)
      endpoint.readsEnabled must equal(true)
      con.receivedData(DataBuffer(ByteString("d")))
      executed must equal(true)






    }

  }

  


}

