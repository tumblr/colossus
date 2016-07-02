package colossus.protocols.http

import akka.util.ByteString
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

}
