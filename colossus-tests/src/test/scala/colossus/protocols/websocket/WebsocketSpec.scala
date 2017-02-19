package colossus
package protocols.websocket

import core.{DataBlock, DataBuffer, ServerContext}

import service.{DecodedResult, Protocol}
import protocols.http._
import java.util.Random

import org.scalatest._
import colossus.testkit._

import akka.util.ByteString

import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}

class WebsocketSpec extends ColossusSpec {

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

  val validResponse = HttpResponse(
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

  "Http Upgrade Request" must {
    "correctly translate key from RFC" in {
      UpgradeRequest.processKey("dGhlIHNhbXBsZSBub25jZQ==") must equal("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")
    }

    "accept a properly crafted upgrade request" in {
      UpgradeRequest.validate(valid, List.empty).isEmpty must equal(false)
    }

    "accept a properly crafted upgrade request with origins" in {
      UpgradeRequest.validate(valid, List("http://foo.bar", "https://foo.bar")).isEmpty must equal(false)
    }

    "decline upgrade request by origins" in {
      UpgradeRequest.validate(valid, List("http://another.foo.bar")).isEmpty must equal(true)
    }

    "produce a correctly formatted response" in {
      UpgradeRequest.validate(valid, List.empty).get must equal(validResponse)

    }
  }

  "frame parsing" must {

    "unmask data" in {
      val masked = DataBlock("abcd") ++ DataBlock(ByteString(41, 7, 15, 8, 14, 66, 52, 11, 19, 14, 7, 69).toArray)
      FrameParser.unmask(true, masked).byteString must equal(ByteString("Hello World!"))
    }

    "parse a frame" in {
      val data = ByteString(-119, -116, 115, 46, 27, -120, 59, 75, 119, -28, 28, 14, 76, -25, 1, 66, 127, -87).toArray
      val expected = Frame(Header(OpCodes.Ping, true), DataBlock("Hello World!"))
      val parsed = FrameParser.frame.parse(DataBuffer(data)) must equal(Some(DecodedResult.Static(expected)))
    }

    "parse its own encoding" in {
      val expected = Frame(Header(OpCodes.Text, true), DataBlock("Hello World!!!!!!"))
      FrameParser.frame.parse(expected.encode(new Random)) must equal(Some(DecodedResult.Static(expected)))
    }


  }

  "frame encoding" must {
    def sized(len: Int) = Frame(Header(OpCodes.Text, false), DataBlock(List.fill(len)("x").mkString)).encode(new Random).bytes

    "handle small payload sizes" in {
      val bytes = sized(125)
      bytes(1) mustBe 0x7D //mask bit unset + 125 length
    }

    "handle medium payload sizes" in {
      val bytes = sized(126)
      bytes.drop(1).take(3) mustBe ByteString(0x7E, 0x00, 0x7E)

      val bytes2 = sized(12543)
      bytes2.drop(1).take(3) mustBe ByteString(0x7E, 0x30, 0xFF)
    }

    "handle large payload sizes" in {
      val bytes = sized(126872)
      bytes.drop(1).take(9) mustBe (ByteString(0x7F, 0, 0, 0, 0, 0, 0x01, 0xEF, 0x98))
    }
  }


  "WebsocketHandler" must {
    //a simple codec to test decoding errors
    trait CString extends Protocol {
      type Input = String
      type Output = String
    }

    class CStringCodec extends FrameCodec[CString] {
      def encode(str: String): DataBlock = DataBlock(":" + str)
      def decode(block: DataBlock): Try[String] = {
        val x = block.utf8String
        if (x.startsWith(":")) {
          Success(x.drop(1))
        } else {
          Failure(new Exception("Bad formatting!"))
        }
      }
    }

    implicit object CStringCodecProvider extends FrameCodecProvider[CString] {
      def provideCodec() = new CStringCodec
    }

    class MyHandler(context: ServerContext) extends WebsocketServerHandler[CString](context) {
      def handle = {
        case "A" => {
          sendMessage("B")
        }
      }

      // Members declared in colossus.protocols.websocket.WebsocketHandler
      def handleError(reason: Throwable): Unit = {
        sendMessage("E")
      }

      // Members declared in colossus.core.WorkerItem
      def receivedMessage(message: Any,sender: akka.actor.ActorRef): Unit = ???
    }
    val random = new java.util.Random

    def sendReceive(send: Frame, expected: Frame) {
      val con = MockConnection.server(new MyHandler(_))
      con.typedHandler.connected(con)
      con.typedHandler.receivedData(send.encode(random))
      con.iterate()
      con.expectOneWrite(ByteString(expected.encode(random).takeAll))
    }

    "properly handle a frame" in {
      val frame = Frame(Header(OpCodes.Text, true), DataBlock(":A"))
      val expectedFrame = Frame(Header(OpCodes.Text, false), DataBlock(":B"))
      sendReceive(frame, expectedFrame)
    }

    "react to malformed data" in {
      val frame = Frame(Header(OpCodes.Text, true), DataBlock("A"))
      val expectedFrame = Frame(Header(OpCodes.Text, false), DataBlock(":E"))
      sendReceive(frame, expectedFrame)
    }

    "ping/pong" in {
      val frame = Frame(Header(OpCodes.Ping, true), DataBlock(""))
      val expectedFrame = Frame(Header(OpCodes.Pong, false), DataBlock(""))
      sendReceive(frame, expectedFrame)
    }

    "close" in {
      val con = MockConnection.server(new MyHandler(_))
      val send = Frame(Header(OpCodes.Close, true), DataBlock(""))
      con.typedHandler.connected(con)
      con.typedHandler.receivedData(send.encode(random))
      con.expectDisconnectAttempt()
    }



  }

  "WebsocketHttp" must {
    import subprotocols.rawstring._
    val myinit = new WebsocketInitializer(FakeIOSystem.fakeWorker.worker) {
      def onConnect = new WebsocketServerHandler[RawString](_) {
        def handle = {
          case "A" => {
            sendMessage("B")
          }
        }
        def handleError(reason: Throwable): Unit = {
          sendMessage("E")
        }
        def receivedMessage(message: Any,sender: akka.actor.ActorRef): Unit = ???
      }
    }

    "switch connection handler on successful upgrade request" in {
      val con = MockConnection.server(new WebsocketHttpHandler(_, myinit, "/foo", List.empty))
      con.typedHandler.connected(con)
      con.typedHandler.receivedData(DataBuffer(valid.bytes))
      con.iterate()
      con.expectOneWrite(validResponse.bytes)
      con.iterate()
      con.workerProbe.expectMsgType[core.WorkerCommand.SwapHandler](100.milliseconds)
    }
    "return 400 and not switch on invalid request" in {
      val bad = HttpRequest.get("/foo")
      val con = MockConnection.server(new WebsocketHttpHandler(_, myinit, "/foo", List.empty))
      con.typedHandler.connected(con)
      con.typedHandler.receivedData(DataBuffer(bad.bytes))
      con.iterate()
      con.withExpectedWrite(_.utf8String.contains("400") mustBe true)
      con.iterate()
      con.workerProbe.expectNoMsg(100.milliseconds)

    }
  }

}

