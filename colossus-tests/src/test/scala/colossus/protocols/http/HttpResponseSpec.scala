package colossus.protocols.http

import akka.util.ByteString
import colossus.testkit.{CallbackMatchers, ColossusSpec, FakeIOSystem}
import org.scalatest.{OptionValues, TryValues}

import scala.concurrent.duration._

class HttpResponseSpec extends ColossusSpec with TryValues with OptionValues with CallbackMatchers {

  implicit val cbe = FakeIOSystem.testExecutor

  implicit val duration = 1.second

  "An HttpResponse" must {

    "be constructable from a ByteStringLike" in {

      val response = HttpResponse(HttpVersion.`1.1`, HttpCodes.ACCEPTED, HttpHeaders(), "test conversion")

      val expected = HttpResponse(HttpVersion.`1.1`, HttpCodes.ACCEPTED, HttpHeaders(), ByteString("test conversion"))

      // currently there's no easy way to pass the contentType into HttpResponse.apply()
      response must equal(expected.withContentType("text/plain"))
    }
  }

  "HttpResponseBuilding" must {

    def expectCode(response: HttpResponse, code: HttpCode) {
      val expected = HttpResponse(HttpVersion.`1.1`, code, HttpHeaders.Empty, HttpBody("hello"))
      response mustBe expected
    }

    "ok" in {
      expectCode(HttpResponse.ok("hello"), HttpCodes.OK)
    }
    "not found" in {
      expectCode(HttpResponse.notFound("hello"), HttpCodes.NOT_FOUND)
    }
    "bad request" in {
      expectCode(HttpResponse.badRequest("hello"), HttpCodes.BAD_REQUEST)
    }
    "error" in {
      expectCode(HttpResponse.error("hello"), HttpCodes.INTERNAL_SERVER_ERROR)
    }
    "forbidden" in {
      expectCode(HttpResponse.forbidden("hello"), HttpCodes.FORBIDDEN)
    }
  }

}
