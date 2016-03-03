package colossus
package protocols.websocket

import core._
import controller._
import service._
import akka.util.{ByteString, ByteStringBuilder}

import java.math.BigInteger
import java.security.MessageDigest
import java.util.Random
import sun.misc.{BASE64Encoder, BASE64Decoder}


object UpgradeRequest {
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
    val headers = request.head.headers
    for {
      cheader   <- headers.singleHeader("connection") 
      uheader   <- headers.singleHeader("upgrade") 
      host      <- headers.singleHeader("host")
      origin    <- headers.singleHeader("origin")
      seckey    <- headers.singleHeader("sec-websocket-key")
      secver    <- headers.singleHeader("sec-websocket-version") 
      if (request.head.version == HttpVersion.`1.1`)
      if (request.head.method == HttpMethod.Get)
      if (secver == "13")
      if (uheader.toLowerCase == "websocket")
      if (cheader.toLowerCase.split(",").map{_.trim} contains "upgrade")
    } yield HttpResponse (
      HttpResponseHead(
        HttpVersion.`1.1`,
        HttpCodes.SWITCHING_PROTOCOLS,
        HttpHeaders(
          HttpHeader("Upgrade", "websocket"),
          HttpHeader("Connection", "Upgrade"),
          HttpHeader("Sec-Websocket-Accept",processKey(seckey))
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

  def encode(random: Random): DataBuffer = {
    val b = new ByteStringBuilder
    b.sizeHint(payload.size + 20)
    b putByte ((header.opcode | 0x80).toByte) //set the fin bit for now every time
    val maskBit: Byte = if (header.mask) 0x80.toByte else 0x00.toByte
    if (payload.size < 126) {
      b putByte (maskBit | payload.size.toByte).toByte
    } else if (payload.size < 65536) {
      b putByte (maskBit | 0x7E).toByte
      b putShort(payload.size)
    } else {
      b putByte (maskBit | 0x7F).toByte
      b putLong(payload.size)
    }
    if (header.mask) {
      val arr = new Array[Byte](4)
      random.nextBytes(arr)
      val mask = ByteString(arr)
      val masked = Frame.mask(mask, payload)
      b append mask
      b append masked
    } else {
      b append payload
    }
    DataBuffer(b.result)
  }
}

object Frame {

  /**
   *
   * This is used for both masking and unmasking
   *
   * mask must be 4 bytes long
   */
  def mask(mask: ByteString, payload: ByteString): ByteString = {
    val builder = new ByteStringBuilder
    builder.sizeHint(payload.size)
    var index = 0
    payload.foreach{ b: Byte =>
      builder putByte (b ^ (mask(index % 4).toByte)).toByte
      index += 1
    }
    builder.result

  }

}


object FrameParser {
  import parsing._
  import Combinators._

  /**
   * When the mask bit is set (always from client messages) we need to XOR the
   * n-th data byte with the (n mod 4)th mask byte
   */
  def unmask(isMasked: Boolean, data: ByteString): ByteString = if (!isMasked) data else {
    val mask = data.take(4)
    val payload = data.drop(4)
    Frame.mask(mask, payload)
  }
  

  
  //see https://tools.ietf.org/html/rfc6455#section-5.2
  def frame = bytes(2) |> {head =>
    val opcode = head(0) & 0x0F
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
    p >> {data => DecodedResult.Static(Frame(Header(opcode.toByte, mask), unmask(mask,data)))}
  }
}

abstract class WebsocketHandler(context: Context) extends Controller(new WebsocketCodec, ControllerConfig(50, scala.concurrent.duration.Duration.Inf), context) {

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
