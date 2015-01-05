package colossus.protocols.http.client

import akka.util.ByteString
import colossus.core.DataBuffer
import colossus.protocols.http._
import colossus.service.DecodedResult
import org.scalatest.{WordSpec, MustMatchers, TryValues, OptionValues}

class StreamingHttpResponseParserSpec extends WordSpec with MustMatchers with TryValues with OptionValues {


  "StreamingHttpResponseParser" must {

    "parse streams" in {

      val content = "{some : json}"

      val sent = HttpResponse(HttpVersion.`1.1`,
        HttpCodes.OK,
        List("host"->"api.foo.bar:444", "accept"->"*/*", "authorization"->"Basic XXX", "accept-encoding"->"gzip, deflate"),
        ByteString("{some : json}"))

      val clientProtocol = HttpClientCodec.streaming()

      val bytes = StaticResponseBuilder(sent)
      val decodedResponse: Option[DecodedResult[StreamingHttpResponse]] = clientProtocol.decode(bytes)

      decodedResponse match {
        case Some(DecodedResult.Streamed(res, s)) => {
          res.stream.pullCB().execute { x => ByteString(x.success.value.value.takeAll) must equal(content)
          }
        }
        case _ => throw new Exception("expected some")
      }

    }

/*    "parse a stream with chunked transfer encoding" in {
      val res = "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n3\r\nfoo\r\ne\r\n123456789abcde\r\n0\r\n\r\n"
      val parser = HttpResponseParser.streaming()

      val parsed = parser.parse(DataBuffer(ByteString(res))).get
      parsed.data must equal (ByteString("foo123456789abcde"))
    }*/


  }

}
