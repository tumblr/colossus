package colossus
package protocols

import core._
import controller._
import service._
import akka.util.{ByteString, ByteStringBuilder}

package object websocket {

  class WebsocketCodec extends Codec[Frame, Frame]{
    
    private val parser = FrameParser.frame

    def decode(data: DataBuffer) = parser.parse(data)

    def encode(f: Frame) = f.encode

    def reset(){}
  }

  trait Websocket extends CodecDSL {
    type Input = Frame
    type Output = Frame
  }

  implicit object WebsocketCodecProvider extends CodecProvider[Websocket] {

    def provideCodec() = new WebsocketCodec

    //TODO : looks like we need to break this out from codec provider
    def errorResponse(request: Frame, reason: Throwable): Frame = ???
  }
    

}

