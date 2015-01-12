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
        List("host"->"api.foo.bar:444", "authorization"->"Basic XXX", "accept-encoding"->"gzip, deflate"),
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

    "parse a stream with chunked transfer encoding" in {
      val res = "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n3\r\nfoo\r\ne\r\n123456789abcde\r\n0\r\n\r\n"

      val p = HttpClientCodec.streaming()
      val parsed: DecodedResult[StreamingHttpResponse] = p.decode(DataBuffer(ByteString(res))).get

      parsed match {
        case DecodedResult.Streamed(x, s) => {
          x.stream.pullCB().execute(x => ByteString(x.success.value.value.takeAll) must equal ("foo123456789abcde"))
        }
        case _ => throw new Exception("expected some")
      }
    }

    "parse a stream with chunked transfer encoding across multiple packets" in {
      val res = "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n"

      val body = "3\r\nfoo\r\ne\r\n123456789abcde\r\n0\r\n\r\n"

      val p = HttpClientCodec.streaming()
      val parsed: DecodedResult[StreamingHttpResponse] = p.decode(DataBuffer(ByteString(res))).get

      parsed match {
        case DecodedResult.Streamed(x, s) => {
          s.push(DataBuffer(ByteString(body)))
          x.stream.pullCB().execute(x => ByteString(x.success.value.value.takeAll) must equal (ByteString("foo123456789abcde")))
        }
        case _ => throw new Exception("expected some")
      }
    }

    "parse a stream with chunked transfer encoding across multiple packets with multiple requests" in {
      val res = "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n"

      val body = "3\r\nfoo\r\ne\r\n123456789abcde\r\n0\r\n\r\n"
      val seq = Seq(ByteString("3"), ByteString("\r\nfoo\r\ne"), ByteString("\r\n123456789abc"))

      val p = HttpClientCodec.streaming()
      val parsed: DecodedResult[StreamingHttpResponse] = p.decode(DataBuffer(ByteString(res))).get

      var callbackExecuted = false

      parsed match {
        case DecodedResult.Streamed(x, s) => {
          s.push(DataBuffer(ByteString(body)))
          seq.foreach{ case y =>
            s.push(DataBuffer(y))
          }
          val last = DataBuffer(ByteString("de\r\n0\r\n\r\nHTTP"))
          s.push(last)
          x.stream.pullCB().execute { x =>
            ByteString(x.success.value.value.takeAll) must equal(ByteString("foo123456789abcde"))
            last.hasUnreadData must equal(true)
            callbackExecuted = true
          }

        }
        case _ => throw new Exception("expected some")
      }

      callbackExecuted must equal (true)
    }
  }

}
