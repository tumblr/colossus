package colossus.protocols.http

import akka.util.ByteString
import colossus.controller.Source
import colossus.core.DataBuffer
import colossus.testkit.{CallbackMatchers, ColossusSpec, FakeIOSystem}
import org.scalatest.{OptionValues, TryValues}

import scala.concurrent.duration._

class HttpResponseSpec extends ColossusSpec with TryValues with OptionValues with CallbackMatchers {

  implicit val cbe = FakeIOSystem.testExecutor

  implicit val duration = 1.second

  "An HttpResponse" must {

    "be constuctable from a ByteStringLike" in {

      val response = HttpResponse(HttpVersion.`1.1`, HttpCodes.ACCEPTED, HttpHeaders(), "test conversion")

      val expected = HttpResponse(HttpVersion.`1.1`, HttpCodes.ACCEPTED, HttpHeaders(), ByteString("test conversion"))

      response must equal(expected)

    }
  }

  "StreamingHttpResponse" must {

    "be constructable from a StaticHttpResponse" in {

      val payload = "look ma, no hands!"
      val res = HttpResponse(HttpVersion.`1.1`, HttpCodes.OK, HttpHeaders(), ByteString(payload))

      val streamed = StreamingHttpResponse.fromStatic(res)

      streamed.body.get.pullCB() must evaluateTo{x : Option[DataBuffer] =>
        ByteString(x.value.takeAll) must equal(res.body.bytes)
      }
    }

    "be constuctable from a ByteStringLike" in {

      val payload = "look ma, no hands!"

      val expected = StreamingHttpResponse(
        HttpResponseHead(HttpVersion.`1.1`, HttpCodes.OK, HttpHeaders(HttpHeader("content-length", "18"))),
        Some(Source.one(DataBuffer(ByteString("test conversion"))))
      )

      val response = StreamingHttpResponse(HttpVersion.`1.1`, HttpCodes.OK, HttpHeaders(), payload)

      response.head must equal(expected.head)

      response.body.get.pullCB() must evaluateTo{x : Option[DataBuffer] =>
        ByteString(x.value.takeAll) must equal(ByteString(payload))
      }

    }
  }

}
