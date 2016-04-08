package colossus
package protocols

import core._
import service._
import akka.util.{ByteString, ByteStringBuilder}
import java.util.Random

package object websocket {

  class WebsocketCodec extends Codec[Frame, Frame]{
    
    private val random = new Random
    private val parser = FrameParser.frame

    def decode(data: DataBuffer) = parser.parse(data)

    def encode(f: Frame) = f.encode(random)

    def reset(){}
  }

  trait Websocket extends Protocol {
    type Input = Frame
    type Output = Frame
  }

  implicit object WebsocketCodecProvider extends CodecProvider[Websocket] {

    def provideCodec() = new WebsocketCodec

  }
    

}

