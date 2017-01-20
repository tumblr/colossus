package colossus
package protocols.websocket

import colossus.metrics.MetricNamespace
import core._
import controller._
import service._
import streaming.{PushResult, Sink}
import akka.util.ByteStringBuilder

import java.security.MessageDigest
import java.util.Random
import scala.util.{Success, Failure}
import sun.misc.BASE64Encoder

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
  def validate(request : HttpRequest): Option[HttpResponse] = {
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
    p >> {data => Frame(Header(opcode.toByte, mask), unmask(mask,DataBlock(data)))}
  }
}

class WebsocketController[E <: Encoding] (val downstream: WebsocketControllerDownstream[E], val frameCodec: FrameCodec[E]) 
extends ControllerDownstream[WebsocketEncoding] with ControllerUpstream[E]
with DownstreamEventHandler[WebsocketControllerDownstream[E]]
with UpstreamEventHandler[ControllerUpstream[WebsocketEncoding]] {

  val controllerConfig = ControllerConfig(50, metricsEnabled = true)
  downstream.setUpstream(this)
  def connection = upstream.connection
  def namespace = downstream.namespace

  val incoming = Sink.open[Frame]{frame => 
    frame.header.opcode match {
      case OpCodes.Binary | OpCodes.Text => frameCodec.decode(frame.payload) match {
        case Success(obj) => {
          downstream.handle(obj)
          PushResult.Ok
        }
        case Failure(err) => {
          downstream.handleError(err)
          PushResult.Ok
        }
      }
      case OpCodes.Ping => {
        upstream.outgoing.push(Frame(Header(OpCodes.Pong, false), frame.payload))
      }
      case OpCodes.Close => {
        upstream.connection.disconnect()
        PushResult.Ok
      }
      case _ => PushResult.Ok
    }
  }

  lazy val outgoing = upstream.outgoing.mapIn[E#Output]{ message => 
    val data = frameCodec.encode(message)
    Frame(Header(OpCodes.Text, false), data)
  }

  private def send(bytes: DataBlock): PushResult = {
    //note - as per the spec, server frames are never masked
    val frame = Frame(Header(OpCodes.Text, false), bytes)
    upstream.outgoing.push(frame)
  }

  def send(message: E#Output) {
    send(frameCodec.encode(message))
  }

}

trait WebsocketControllerDownstream[E <: Encoding] extends UpstreamEvents with HasUpstream[ControllerUpstream[E]] with DownstreamEvents {
 
  def handle: PartialFunction[E#Input, Unit]

  def handleError(reason: Throwable)

  def namespace: MetricNamespace

}


abstract class WebsocketHandler[E <: Encoding](val context: Context)
extends WebsocketControllerDownstream[E] with UpstreamEventHandler[ControllerUpstream[E]] with HandlerTail {

  def send(message: E#Output): PushResult = {
    upstream.outgoing.push(message)
  }

}

abstract class WebsocketServerHandler[E <: Encoding](serverContext: ServerContext) extends WebsocketHandler[E](serverContext.context) {
  val namespace = serverContext.server.namespace
}


abstract class WebsocketInitializer[E <: Encoding](val worker: WorkerRef) {

  def provideCodec(): FrameCodec[E]
  
  def onConnect: ServerContext => WebsocketHandler[E]


  def fullHandler(w: WebsocketHandler[E]): ServerConnectionHandler = {
    new PipelineHandler(
      new Controller(
        new WebsocketController(
          w,
          provideCodec()
        ),
        new WebsocketCodec
      ),
      w
    )
  }
        

}

class WebsocketHttpHandler[E <: Encoding](ctx: ServerContext, websocketInit: WebsocketInitializer[E], upgradePath: String)
extends protocols.http.server.RequestHandler(ServiceConfig.Default, ctx) {
  def handle = {
    case request if (request.head.path == upgradePath) => {
      val response = UpgradeRequest.validate(request) match {
        case Some(upgrade) => {
          connection.become(() => websocketInit.fullHandler(websocketInit.onConnect(ctx)))
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
  import server._



  def start[E <: Encoding](name: String, port: Int, upgradePath: String = "/")(init: WorkerRef => WebsocketInitializer[E])(implicit io: IOSystem) = {
    HttpServer.start(name, port){context => new Initializer(context) {
    
      val websockinit : WebsocketInitializer[E] = init(context.worker)

      def onConnect = new WebsocketHttpHandler(_, websockinit, upgradePath)
      
    }}
  }

}

