package colossus.protocols.http

import akka.util.ByteString
import org.scalatest.{MustMatchers, WordSpec}

class HttpResponseSpec extends WordSpec with MustMatchers {

  "An HttpResponse" must {

    "be constuctable from a ByteStringLike" in {

      import ByteStringConverters._

      val response = HttpResponse.fromValue(HttpVersion.`1.1`, HttpCodes.ACCEPTED, Nil, "test conversion")

      val expected = HttpResponse(HttpVersion.`1.1`, HttpCodes.ACCEPTED, Nil, ByteString("test conversion"))

      response must equal(expected)

    }
  }

}
