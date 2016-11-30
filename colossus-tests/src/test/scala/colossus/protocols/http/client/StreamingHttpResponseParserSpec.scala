package colossus.protocols.http.client

import akka.util.ByteString
import colossus.controller._
import colossus.core.{DataBuffer, DataReader, DataStream, DynamicOutBuffer}
import colossus.protocols.http._
import colossus.service.{Callback, DecodedResult}
import colossus.testkit.{CallbackMatchers, ColossusSpec, FakeIOSystem, PipeFoldTester}
import org.scalatest.{MustMatchers, OptionValues, TryValues}

import scala.concurrent.duration._

class StreamingHttpResponseParserSpec extends ColossusSpec with MustMatchers with TryValues with OptionValues with CallbackMatchers {

  implicit val cbe = FakeIOSystem.testExecutor

  implicit val duration = 1.second
  import HttpHeader.Conversions._

  "StreamingHttpResponseParser" must {

    "parse streams" in {

      val content = "{some : json}"

      val sent = HttpResponse(
        HttpVersion.`1.1`,
        HttpCodes.OK,
        Vector("host"->"api.foo.bar:444", "authorization"->"Basic XXX", "accept-encoding"->"gzip, deflate"),
        ByteString("{some : json}")
      )

      val clientProtocol = new StreamingHttpClientCodec()

      val bytes = DataBuffer(sent.bytes)
      val decodedResponse: Option[DecodedResult[StreamingHttpResponse]] = clientProtocol.decode(bytes)

      decodedResponse match {
        case Some(DecodedResult.Stream(res, s)) => {
          println(bytes.remaining)
          s.push(bytes) match {
            case PushResult.Full(trig) => trig.fill{() => s.push(bytes)}
            case _ => throw new Exception("wrong result")
          }
          res.body.get.pullCB() must evaluateTo { x : Option[DataBuffer] =>
            ByteString(x.value.takeAll) must equal (ByteString(content))
            bytes.hasUnreadData must be (false)
          }
        }
        case _ => throw new Exception("expected some")
      }

    }

    "passthrough a stream with chunked transfer encoding" in {
      val head = ByteString("HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n")
      val body = ByteString("3\r\nfoo\r\ne\r\n123456789abcde\r\n0\r\n\r\n")
      val res = head ++ body
      val buffer = DataBuffer(res)
      expectStreamBody(buffer, Some(body), false)
    }

    "parse a stream with chunked transfer encoding" in {
      val res = "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n3\r\nfoo\r\ne\r\n123456789abcde\r\n0\r\n\r\n"
      val buffer = DataBuffer(ByteString(res))
      expectStreamBody(buffer, Some(ByteString("foo123456789abcde")), true)
    }

    "parse a stream with chunked transfer encoding across multiple packets" ignore {
      val res = "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n"

      val body = "3\r\nfoo\r\ne\r\n123456789abcde\r\n0\r\n\r\n"

      val p = new StreamingHttpClientCodec()
      val parsed: DecodedResult[StreamingHttpResponse] = p.decode(DataBuffer(ByteString(res))).get

      parsed match {
        case DecodedResult.Stream(x, s) => {
          val buffer = DataBuffer(ByteString(body))
          s.push(buffer)
          x.body.get.pullCB() must evaluateTo{ x : Option[DataBuffer] =>
            ByteString(x.value.takeAll) must equal (ByteString("foo123456789abcde"))
            buffer.hasUnreadData must be (false)
          }
        }
        case _ => throw new Exception("expected some")
      }
    }

    "parse a stream with chunked transfer encoding across multiple packets with multiple requests" ignore {
      val res = "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n"

      val body = "3\r\nfoo\r\ne\r\n123456789abcde\r\n0\r\n\r\n"
      val seq = Seq(ByteString("3"), ByteString("\r\nfoo\r\ne"), ByteString("\r\n123456789abc"))

      val p = new StreamingHttpClientCodec()
      val parsed: DecodedResult[StreamingHttpResponse] = p.decode(DataBuffer(ByteString(res))).get

      parsed match {
        case DecodedResult.Stream(x, s) => {
          s.push(DataBuffer(ByteString(body)))
          seq.foreach{ case y =>
            s.push(DataBuffer(y))
          }
          val last = DataBuffer(ByteString("de\r\n0\r\n\r\nHTTP"))
          s.push(last)
          x.body.get.pullCB() must evaluateTo { x : Option[DataBuffer] =>
            ByteString(x.value.takeAll) must equal(ByteString("foo123456789abcde"))
            last.hasUnreadData must equal(true)
          }
        }
        case _ => throw new Exception("expected some")
      }

    }
    "decode a response that was encoded by colossus with no body" in {
      validateEncodedStreamingHttpResponse(None)
    }

    "decode a response that was encoded by colossus with a body" in {
      validateEncodedStreamingHttpResponse(Some(ByteString("body bytes!")))
    }

  }

  ////////////////////////// HELPER METHODS ///////////////////////

  private def expectStreamBody(raw: DataBuffer, body: Option[ByteString], dechunk: Boolean) {
    val codec = new StreamingHttpClientCodec(dechunk)
    codec.decode(raw) match {
      case Some(DecodedResult.Stream(response, sink)) => response.body match{
        case Some(source) => {
          val expected = body.getOrElse(throw new Exception(s"Expected no body, but got one"))
          val t = new PipeFoldTester(source.fold(ByteString())(PipeFoldTester.byteFolder))
          sink.push(raw)
          t.expect(expected)
        }
        case None => if (body.isDefined) {
          throw new Exception("expected body but got none")
        }
      }
      case Some(DecodedResult.Static(StreamingHttpResponse(head, None))) => if (body.isDefined) {
        throw new Exception("expected body but got none(static)")
      }
      case other => throw new Exception(s"Invalid parse result $other")
    }
  }

  private def validateEncodedStreamingHttpResponse(body : Option[ByteString]){
    val res = StreamingHttpResponse(
      HttpResponseHead(HttpVersion.`1.1`, HttpCodes.OK, Vector("a"->"b", "content-length"->body.map{_.size.toString}.getOrElse("0"))),
      body.map{bod => Source.one(DataBuffer(bod))}
    )
    val clientProtocol = new StreamingHttpClientCodec
    val serverProtocol = new StreamingHttpServerCodec

    val encodedResponse: DataReader = serverProtocol.encode(res)

    val folded: Callback[ByteString] = encodedResponse match {
      case DataStream(source) => source.fold(ByteString())(PipeFoldTester.byteFolder)
      case d: DataBuffer => Callback.successful(ByteString(d.takeAll))
    }

    folded must evaluateTo { x : ByteString =>
      val buffer = DataBuffer(x)
      expectStreamBody(buffer, body, true)
    }
  }
}


