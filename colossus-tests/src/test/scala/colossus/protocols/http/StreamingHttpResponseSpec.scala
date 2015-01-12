package colossus.protocols.http

import akka.util.ByteString
import org.scalatest.{OptionValues, TryValues, MustMatchers, WordSpec}

class StreamingHttpResponseSpec extends WordSpec with MustMatchers with TryValues with OptionValues{

  "StreamingHttpResponse" must {

    "be constructable from a StaticHttpResponse" in {

      val payload = "look ma, no hands!"
      val res = HttpResponse(HttpVersion.`1.1`, HttpCodes.OK, Nil, ByteString(payload))

      val streamed = StreamingHttpResponse.fromStatic(res)

      streamed.stream.pullCB().execute { x => ByteString(x.success.value.value.takeAll) must equal(res.data)}
    }
  }
}
