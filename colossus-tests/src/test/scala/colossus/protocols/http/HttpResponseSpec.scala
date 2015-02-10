package colossus.protocols.http

import akka.util.ByteString
import colossus.controller.Source
import colossus.core.DataBuffer
import colossus.testkit.CallbackMatchers
import org.scalatest.{OptionValues, TryValues, MustMatchers, WordSpec}

class HttpResponseSpec extends WordSpec with MustMatchers with TryValues with OptionValues with CallbackMatchers {

  "An HttpResponse" must {

    "be constuctable from a ByteStringLike" in {

      import ByteStringConverters._

      val response = HttpResponse.fromValue(HttpVersion.`1.1`, HttpCodes.ACCEPTED, Nil, "test conversion")

      val expected = HttpResponse(HttpVersion.`1.1`, HttpCodes.ACCEPTED, Nil, ByteString("test conversion"))

      response must equal(expected)

    }
  }

  "StreamingHttpResponse" must {

    "be constructable from a StaticHttpResponse" in {

      val payload = "look ma, no hands!"
      val res = HttpResponse(HttpVersion.`1.1`, HttpCodes.OK, Nil, ByteString(payload))

      val streamed = StreamingHttpResponse.fromStatic(res)

      streamed.stream.pullCB() must evaluateTo{x : Option[DataBuffer] =>
        ByteString(x.value.takeAll) must equal(res.data)
      }
    }

    "be constuctable from a ByteStringLike" in {

      import ByteStringConverters._

      val payload = "look ma, no hands!"

      val expected = StreamingHttpResponse(HttpVersion.`1.1`, HttpCodes.OK, Vector("content-length"->"18"), Source.one(DataBuffer(ByteString("test conversion"))))

      val response = StreamingHttpResponse.fromValue(HttpVersion.`1.1`, HttpCodes.OK, Nil, payload)

      response must equal(expected)

      response.stream.pullCB() must evaluateTo{x : Option[DataBuffer] =>
        ByteString(x.value.takeAll) must equal(ByteString(payload))
      }

    }
  }

}
