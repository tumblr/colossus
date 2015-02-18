package colossus.protocols.http.client

import akka.util.ByteString
import colossus.controller._
import colossus.core.{DataBuffer, DataStream, DataReader}
import colossus.protocols.http._
import colossus.service.{Callback, DecodedResult}
import colossus.testkit.CallbackMatchers
import org.scalatest.{WordSpec, MustMatchers, TryValues, OptionValues}

class StreamingHttpResponseParserSpec extends WordSpec with MustMatchers with TryValues with OptionValues with CallbackMatchers {


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
          s.push(bytes) match {
            case PushResult.Full(trig) => trig.fill{() => s.push(bytes)}
            case _ => throw new Exception("wrong result")
          }
          res.stream.pullCB() must evaluateTo { x : Option[DataBuffer] =>
            ByteString(x.value.takeAll) must equal (ByteString(content))
            bytes.hasUnreadData must be (false)
          }
        }
        case _ => throw new Exception("expected some")
      }

    }

    "parse a stream with chunked transfer encoding" in {
      val res = "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n3\r\nfoo\r\ne\r\n123456789abcde\r\n0\r\n\r\n"

      val buffer = DataBuffer(ByteString(res))

      val p = HttpClientCodec.streaming()
      val parsed: DecodedResult[StreamingHttpResponse] = p.decode(buffer).get
      parsed match {
        case DecodedResult.Streamed(x, s) => {
          s.push(buffer)
          x.stream.pullCB() must evaluateTo { y : Option[DataBuffer] =>
            ByteString(y.value.takeAll) must equal (ByteString("foo123456789abcde"))
            buffer.hasUnreadData must be (false)
          }
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
          val buffer = DataBuffer(ByteString(body))
          s.push(buffer)
          x.stream.pullCB() must evaluateTo{ x : Option[DataBuffer] =>
            ByteString(x.value.takeAll) must equal (ByteString("foo123456789abcde"))
            buffer.hasUnreadData must be (false)
          }
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

      parsed match {
        case DecodedResult.Streamed(x, s) => {
          s.push(DataBuffer(ByteString(body)))
          seq.foreach{ case y =>
            s.push(DataBuffer(y))
          }
          val last = DataBuffer(ByteString("de\r\n0\r\n\r\nHTTP"))
          s.push(last)
          x.stream.pullCB() must evaluateTo { x : Option[DataBuffer] =>
            ByteString(x.value.takeAll) must equal(ByteString("foo123456789abcde"))
            last.hasUnreadData must equal(true)
          }
        }
        case _ => throw new Exception("expected some")
      }

    }
    "decode a response that was encoded by colossus with no body" in {
      validateEncodedStreamingHttpResponse(ByteString(""))
    }

    "decode a response that was encoded by colossus with a body" in {
      validateEncodedStreamingHttpResponse(ByteString("body bytes!"))
    }

  }

  private def validateEncodedStreamingHttpResponse(body : ByteString){
    val res = StreamingHttpResponse(HttpVersion.`1.1`, HttpCodes.OK,
      Vector("a"->"b", "content-length"->body.size.toString), Source.one(DataBuffer(body)))
    val clientProtocol = HttpClientCodec.streaming()
    val serverProtocol =  HttpServerCodec.streaming

    val encodedResponse: DataReader = serverProtocol.encode(res)
    encodedResponse mustBe a [DataStream]

    val source : Source[DataBuffer] = encodedResponse.asInstanceOf[DataStream].source
    val folded = source.fold(ByteString(""))((buffer, bytes) => bytes ++ ByteString(buffer.takeAll))

    folded must evaluateTo { x : ByteString =>
      val buffer = DataBuffer(x)
      val decoded: Option[DecodedResult[StreamingHttpResponse]] = clientProtocol.decode(buffer)
      decoded match {
        case Some(DecodedResult.Streamed(resp, s)) => {
          s.push(buffer) match {
            case PushResult.Closed => {}
            case PushResult.Full(trig) => trig.fill{() => s.push(buffer)}
            case other => throw new Exception(s"wrong push result $other")
          }
          resp must equal (res)
          val f2: Callback[ByteString] = resp.stream.fold(ByteString(""))((buffer, bytes) => bytes ++ ByteString(buffer.takeAll))
          f2 must evaluateTo{x : ByteString =>
             x must equal(body)
          }
        }
        case _ => throw new Exception("expected streamed http response")
      }
    }
  }
}


