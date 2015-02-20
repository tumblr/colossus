package colossus.protocols.http.client

import akka.util.ByteString
import colossus.controller._
import colossus.core.{DataBuffer, DataStream, DataReader}
import colossus.protocols.http._
import colossus.service.{Callback, DecodedResult}
import colossus.testkit.CallbackMatchers
import org.scalatest.{WordSpec, MustMatchers, TryValues, OptionValues}
import scala.util.{Try, Success, Failure}

class StreamingHttpResponseParserSpec extends WordSpec with MustMatchers with TryValues with OptionValues with CallbackMatchers {


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

      val bytes = sent.encode()
      val decodedResponse: Option[DecodedResult[StreamingHttpResponse]] = clientProtocol.decode(bytes)

      decodedResponse match {
        case Some(DecodedResult.Stream(res, s)) => {
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

      val p = new StreamingHttpClientCodec()
      val parsed: DecodedResult[StreamingHttpResponse] = p.decode(buffer).get
      parsed match {
        case DecodedResult.Stream(response, sink) => {
          var pulled = ByteString("WRONG")
          response.body.get.fold(ByteString()){(dataBuffer, build) => build ++ ByteString(dataBuffer.takeAll)}.execute{
            case Success(data) => pulled = data
            case Failure(err) => throw err
          }
          sink.push(buffer) must equal(PushResult.Complete)
          pulled must equal(body)
        }
        case _ => throw new Exception("Expected Stream")
      }


    }

    "parse a stream with chunked transfer encoding" ignore {
      val res = "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n3\r\nfoo\r\ne\r\n123456789abcde\r\n0\r\n\r\n"

      val buffer = DataBuffer(ByteString(res))

      val p = new StreamingHttpClientCodec(dechunk = true)
      val parsed: DecodedResult[StreamingHttpResponse] = p.decode(buffer).get
      parsed match {
        case DecodedResult.Stream(x, s) => {
          //var actual = ByteString("WRONG")
          s.push(buffer)
          x.body.get.pullCB() must evaluateTo { y : Option[DataBuffer] =>
            ByteString(y.value.takeAll) must equal (ByteString("foo123456789abcde"))
            buffer.hasUnreadData must be (false)
          }
        }
        case _ => throw new Exception("expected some")
      }
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
    "decode a response that was encoded by colossus with no body" ignore {
      validateEncodedStreamingHttpResponse(ByteString(""))
    }

    "decode a response that was encoded by colossus with a body" ignore {
      validateEncodedStreamingHttpResponse(ByteString("body bytes!"))
    }

  }

  private def validateEncodedStreamingHttpResponse(body : ByteString){
    val res = StreamingHttpResponse(
      HttpResponseHead(HttpVersion.`1.1`, HttpCodes.OK, Vector("a"->"b", "content-length"->body.size.toString)), 
      Some(Source.one(DataBuffer(body)))
    )
    val clientProtocol = new StreamingHttpClientCodec
    val serverProtocol = new StreamingHttpServerCodec

    val encodedResponse: DataReader = serverProtocol.encode(res)
    encodedResponse mustBe a [DataStream]

    val source : Source[DataBuffer] = encodedResponse.asInstanceOf[DataStream].source
    val folded = source.fold(ByteString(""))((buffer, bytes) => bytes ++ ByteString(buffer.takeAll))

    folded must evaluateTo { x : ByteString =>
      val buffer = DataBuffer(x)
      val decoded: Option[DecodedResult[StreamingHttpResponse]] = clientProtocol.decode(buffer)
      decoded match {
        case Some(DecodedResult.Stream(resp, s)) => {
          s.push(buffer) match {
            case PushResult.Closed => {}
            case PushResult.Full(trig) => trig.fill{() => s.push(buffer)}
            case other => throw new Exception(s"wrong push result $other")
          }
          resp must equal (res)
          val f2: Callback[ByteString] = resp.body.get.fold(ByteString("")){(buffer, bytes) => bytes ++ ByteString(buffer.takeAll)}
          f2 must evaluateTo{x : ByteString =>
             x must equal(body)
          }
        }
        case _ => throw new Exception("expected streamed http response")
      }
    }
  }
}


