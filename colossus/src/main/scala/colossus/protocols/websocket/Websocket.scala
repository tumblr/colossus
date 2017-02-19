package colossus
package protocols.websocket

import core._
import controller._
import service._
import akka.util.{ByteString, ByteStringBuilder}

import java.math.BigInteger
import java.security.MessageDigest
import java.util.Random
import scala.util.{Try, Success, Failure}
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


  /**
   * Validate a HttpRequest as a websocket upgrade request, returning the
   * properly formed response that should be sent back to confirm the upgrade
   */
  def validate(request : HttpRequest, origins: List[String]): Option[HttpResponse] = {
    val headers = request.head.headers
    for {
      cheader   <- headers.firstValue("connection")
      uheader   <- headers.firstValue("upgrade")
      host      <- headers.firstValue("host")
      origin    <- headers.firstValue("origin")
      seckey    <- headers.firstValue("sec-websocket-key")
      secver    <- headers.firstValue("sec-websocket-version")
      if (request.head.version  == HttpVersion.`1.1`)
      if (request.head.method   == HttpMethod.Get)
      if (secver == "13")
      if (uheader.toLowerCase == "websocket")
      if (cheader.toLowerCase.split(",").map{_.trim} contains "upgrade")
      if (origins.isEmpty || headers.firstValue("origin").exists(origins.contains))
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
      HttpBody.NoBody
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

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

}

/**
 * A Parsed Frame Header
 */
case class Header(opcode: Byte, mask: Boolean)

/**
 * A Frame represents a single chunk of data sent by either the client or server.
 */
case class Frame(header: Header, payload: DataBlock) {
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
      val mask = new Array[Byte](4)
      random.nextBytes(mask)
      val masked = Frame.mask(DataBlock(mask), payload)
      b putBytes mask
      b putBytes masked.data
    } else {
      b putBytes payload.data
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
  def mask(mask: DataBlock, payload: DataBlock): DataBlock = {
    val build = new Array[Byte](payload.length)
    var index = 0
    while (index < payload.length) {
      build(index) = (payload(index) ^ (mask(index % 4).toByte)).toByte
      index += 1
    }
    DataBlock(build)
  }

}


object FrameParser {
  import parsing._
  import Combinators._

  /**
   * When the mask bit is set (always from client messages) we need to XOR the
   * n-th data byte with the (n mod 4)th mask byte
   */
  def unmask(isMasked: Boolean, data: DataBlock): DataBlock = if (!isMasked) data else {
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
    val p: Parser[Array[Byte]] = if (payloadLen == 0x7E) {
      bytes(short >> {_ + maskKeyBytes})
    } else if (payloadLen == 0x7F) {
      bytes(long >> {_.toInt + maskKeyBytes})
    } else {
      bytes(payloadLen + maskKeyBytes)
    }
    p >> {data => DecodedResult.Static(Frame(Header(opcode.toByte, mask), unmask(mask,DataBlock(data))))}
  }
}

abstract class BaseWebsocketHandler(context: Context, config: ControllerConfig)
  extends Controller(new WebsocketCodec, config, context) {

  def send(bytes: DataBlock)(postWrite: OutputResult => Unit): Boolean = {
    //note - as per the spec, server frames are never masked
    push(Frame(Header(OpCodes.Text, false), bytes))(postWrite)
  }

  def processMessage(message: Frame)

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



/**
 * A websocket server connection handler that uses a specified sub-protocol.
 */
abstract class WebsocketHandler[P <: Protocol](context: Context, config: ControllerConfig)
(implicit provider: FrameCodecProvider[P]) extends BaseWebsocketHandler(context, config) {

  val wscodec = provider.provideCodec()

  def handle: PartialFunction[P#Input, Unit]

  def handleError(reason: Throwable)

  def sendMessage(message: P#Output)(implicit postWrite: OutputResult => Unit = WebsocketHandler.NoopPostWrite): Boolean =  {
    send(wscodec.encode(message))(postWrite)
  }

  def processMessage(frame: Frame) {
    frame.header.opcode match {
      case OpCodes.Binary | OpCodes.Text => wscodec.decode(frame.payload) match {
        case Success(obj) => handle(obj)
        case Failure(err) => handleError(err)
      }
      case OpCodes.Ping => {
        push(Frame(Header(OpCodes.Pong, false), frame.payload))(WebsocketHandler.NoopPostWrite)
      }
      case OpCodes.Close => {
        disconnect()
      }
      case _ => {}
    }
  }
}

object WebsocketHandler {
  val DefaultConfig = ControllerConfig(1024, scala.concurrent.duration.Duration.Inf)

  val NoopPostWrite: OutputResult => Unit = _ => ()
}


abstract class WebsocketServerHandler[P <: Protocol](serverContext: ServerContext, config: ControllerConfig)(implicit provider: FrameCodecProvider[P])
extends WebsocketHandler[P](serverContext.context, config)(provider) with ServerConnectionHandler {

  def this(serverContext: ServerContext) (implicit provider: FrameCodecProvider[P]) = this(serverContext, WebsocketHandler.DefaultConfig)

  implicit val namespace = serverContext.server.namespace

}

abstract class WebsocketInitializer(val worker: WorkerRef) {

  def onConnect: ServerContext => BaseWebsocketHandler

}

class WebsocketHttpHandler(ctx: ServerContext, websocketInit: WebsocketInitializer, upgradePath: String, origins: List[String])
extends protocols.http.HttpService(ServiceConfig.Default, ctx) {
  def handle = {
    case request if (request.head.path == upgradePath)=> {
      val response = UpgradeRequest.validate(request, origins) match {
        case Some(upgrade) => {
          become(() => websocketInit.onConnect(ctx))
          upgrade
        }
        case None => {
          request.badRequest("Invalid upgrade request")
        }
      }
      Callback.successful(response)
    }
  }
}

object WebsocketServer {
  import protocols.http._

  /**
   * Start a Websocket server on the specified port.  Since Websocket
   * connections are upgraded from HTTP connections, this will actually start an
   * HTTP server and react to Websocket upgrade requests on the path
   * `upgradePath`, all other paths will 404.
   */
  def start(name: String, port: Int, upgradePath: String = "/", origins: List[String] = List.empty)(init: WorkerRef => WebsocketInitializer)(implicit io: IOSystem) = {
    Server.start(name, port){worker => new Initializer(worker) {

      val websockinit : WebsocketInitializer = init(worker)

      def onConnect = new WebsocketHttpHandler(_, websockinit, upgradePath, origins)

    }}
  }

}

