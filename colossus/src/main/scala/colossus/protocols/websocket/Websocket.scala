package colossus
package protocols.websocket

import core._
import controller._
import service._
import akka.util.{ByteString, ByteStringBuilder}

import java.math.BigInteger
import java.security.MessageDigest
import sun.misc.{BASE64Encoder, BASE64Decoder}


object WebsocketUpgradeRequest {
  import protocols.http._

  val salt = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11" //GUID for websocket
  
  val b64 = new BASE64Encoder

  def processKey(key: String): String = {
    val digest = MessageDigest.getInstance("SHA-1")
    digest.reset()
    digest.update((key + salt).getBytes("UTF-8"))
    new String(b64.encode(digest.digest()))
  }
  

  //todo proper handling of key
  def unapply(request : HttpRequest): Option[HttpResponse] = {
    for {
      cheader   <- request.head.singleHeader("connection") 
      uheader   <- request.head.singleHeader("upgrade") 
      host      <- request.head.singleHeader("host")
      origin    <- request.head.singleHeader("origin")
      seckey    <- request.head.singleHeader("sec-websocket-key")
      secver    <- request.head.singleHeader("sec-websocket-version") 
      if (request.head.version == HttpVersion.`1.1`)
      if (request.head.method == HttpMethod.Get)
      if (secver == "13")
      if (uheader.toLowerCase == "websocket")
      if (cheader.toLowerCase == "upgrade")
    } yield HttpResponse (
      HttpResponseHead(
        HttpVersion.`1.1`,
        HttpCodes.SWITCHING_PROTOCOLS,
        Vector(
          HttpResponseHeader("Upgrade", "websocket"),
          HttpResponseHeader("Connection", "Upgrade"),
          HttpResponseHeader("Sec-Websocket-Accept",processKey(seckey))
        )
      ),
      None
    )      
  }
}

object OpCodes {

  type OpCode = Byte

  val Continue  = 0x00.toByte
  val Text      = 0x01.toByte
  val Binary    = 0x02.toByte
  val Close     = 0x08.toByte
  val Ping      = 0x09.toByte
  val Pong      = 0x0A.toByte

  def fromHeaderByte(b: Byte) = b & 0x0F
  
  implicit val byteOrder = java.nio.ByteOrder.nativeOrder()

}

/**
 * A Parsed Frame Header
 */
case class Header(opcode: Byte, mask: Boolean)

case class Frame(header: Header, payload: ByteString) {
  import OpCodes.byteOrder

  def encode: DataBuffer = {
    val b = new ByteStringBuilder
    b.sizeHint(payload.size + 20)
    b putByte ((header.opcode | 0x80).toByte) //set the fin bit for now every time
    if (payload.size < 126) {
      b putByte payload.size.toByte
    } else if (payload.size < 65536) {
      b putByte 0x7E
      b putShort(payload.size)
    } else {
      b putByte 0x7F
      b putLong(payload.size)
    }
    b append payload
    DataBuffer(b.result)
  }
}


object FrameParser {
  import parsing._
  import Combinators._

  def unmask(isMasked: Boolean, data: ByteString): ByteString = if (!isMasked) data else {
    val mask = data.take(4)
    val payload = data.drop(4)
    val builder = new ByteStringBuilder
    builder.sizeHint(payload.size)
    var index = 0
    payload.foreach{ b: Byte =>
      builder putByte (b ^ (mask(index % 4).toByte)).toByte
      index += 1
    }
    builder.result
  }

  
  def frame = bytes(2) |> {head =>
    val payloadLen = head(1) & 0x7F //toss the first bit, len is 7 bits
    val mask = (head(1) & 0x80) == 0x80
    val maskKeyBytes = if (mask) 4 else 0
    val p: Parser[ByteString] = if (payloadLen == 0x7E) {
      bytes(short >> {_.toLong + maskKeyBytes})
    } else if (payloadLen == 0x7F) {
      bytes(long >> {_ + maskKeyBytes})
    } else {
      bytes(payloadLen + maskKeyBytes)
    }
    p >> {data => DecodedResult.Static(Frame(Header(0, mask), unmask(mask,data)))}
  }
}

abstract class WebsocketHandler(context: Context) extends Controller(new WebsocketCodec, ControllerConfig(50, 16384, scala.concurrent.duration.Duration.Inf), context) {

  def send(bytes: ByteString) {
    push(Frame(Header(OpCodes.Text, false), bytes)){_ => {}}
  }

  def handle: PartialFunction[ByteString, Unit]

  private def fullHandler: PartialFunction[ByteString, Unit] = handle orElse {case _ => {}}

  def processMessage(message: Frame) = {
    fullHandler(message.payload)
  }

  //TODO : generalize the stuff in Service and use it for this and for Task as well
  def receivedMessage(message: Any, sender: akka.actor.ActorRef){}

  def preStart(){}

  def postStop(){}

  override def connected(e: WriteEndpoint) {
    super.connected(e)
    preStart()
  }

  override def connectionTerminated(cause: DisconnectCause) {
    super.connectionTerminated(cause)
    postStop()
  }

}
