package colossus
package protocols

import core._
import controller.Encoding
import java.util.Random
import controller.Codec

/**
 * **This package is experimental and subject to breaking changes between
 * versions**.  We currently don't support the complete Websocket spec but there
 * is enough support for basic servers
 *
 * The Websocket protocol defines an asynchronous application protocol.  Since
 * unlike HTTP or other similar protocols there are no requests or responses,
 * writing a Websocket server is slightly different from other service-protocol
 * servers.  In particular, you should use the [[WebsocketServer]] to start a
 * server and the [[WebsocketServerHandler]] to define a connection handler.
 *
 * The [[subprotocols]] package contains some basic sub-protocols, but these
 * tend to be application specific so support is limited.
 *
 * Websocket connection handlers are designed to behave like actors, and mixing
 * in the `ProxyActor` trait can make Websocket connections behave like and
 * interact with Akka actors.
 */
package object websocket {

  class WebsocketCodec extends Codec[WebsocketEncoding]{
    
    private val random = new Random
    private val parser = FrameParser.frame

    def decode(data: DataBuffer) = parser.parse(data)

    def encode(f: Frame, buffer: DataOutBuffer) { buffer write f.encode(random) }

    def endOfStream = None

    def reset(){}
  }

  trait WebsocketEncoding extends Encoding {
    type Input = Frame
    type Output = Frame
  }

}

