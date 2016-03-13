package colossus
package protocols.websocket

import core.DataBuffer
import service.DecodedResult
import protocols.http._
import java.util.Random

import org.scalatest._

import akka.util.ByteString

import scala.util.Success

class WebsocketSpec extends WordSpec with MustMatchers{

  import HttpHeader.Conversions._

  val valid = HttpRequest(
    HttpRequestHead(HttpMethod.Get, "/foo", HttpVersion.`1.1`, Vector(
      ("connection", "Upgrade"),
      ("upgrade", "Websocket"),
      ("sec-websocket-version", "13"),
      ("sec-websocket-key", "rrBE1CeTUMlALwoQxfmTfg=="),
      ("host" , "foo.bar"),
      ("origin", "http://foo.bar")
    )),
    HttpBody.NoBody
  )

  "Http Upgrade Request" must {
    "correctly translate key from RFC" in {
      UpgradeRequest.processKey("dGhlIHNhbXBsZSBub25jZQ==") must equal("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")
    }

    "accept a properly crafted upgrade request" in {
      UpgradeRequest.unapply(valid).isEmpty must equal(false)
    }

    "produce a correctly formatted response" in {
      val expected = HttpResponse(
        HttpResponseHead(
          HttpVersion.`1.1`,
          HttpCodes.SWITCHING_PROTOCOLS,
          HttpHeaders(
            HttpHeader("Upgrade", "websocket"),
            HttpHeader("Connection", "Upgrade"),
            HttpHeader("Sec-Websocket-Accept","MeFiDAjivCOffr7Pn3T2DM7eJHo=")
          )
        ),
        HttpBody.NoBody
      )
      UpgradeRequest.unapply(valid).get must equal(expected)

    }
  }

  "frame parsing" must {

    "unmask data" in {
      val masked = ByteString("abcd") ++ ByteString(41, 7, 15, 8, 14, 66, 52, 11, 19, 14, 7, 69)
      FrameParser.unmask(true, masked) must equal(ByteString("Hello World!"))
    }
    
    "parse a frame" in {
      val data = ByteString(-119, -116, 115, 46, 27, -120, 59, 75, 119, -28, 28, 14, 76, -25, 1, 66, 127, -87)
      val expected = Frame(Header(OpCodes.Ping, true), ByteString("Hello World!"))
      FrameParser.frame.parse(DataBuffer(data)) must equal(Some(DecodedResult.Static(expected)))
    }

    "parse its own encoding" in {
      val expected = Frame(Header(OpCodes.Text, true), ByteString("Hello World!!!!!!"))
      FrameParser.frame.parse(expected.encode(new Random)) must equal(Some(DecodedResult.Static(expected)))
    }
  }

}

