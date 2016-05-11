package colossus
package protocols.websocket

import service._

import scala.util.{Try, Success, Failure}

import core.DataBlock._


trait FrameCodec[P <: Protocol] {

  def decode(data: DataBlock): Try[P#Input]
  def encode(output: P#Output): DataBlock
}

object subprotocols {

  object rawstring {
    
    trait RawString extends Protocol {
      type Input = String
      type Output = String
    }

    class RawStringCodec extends FrameCodec[RawString] {
      def encode(str: String): DataBlock = str.getBytes("UTF-8")
      def decode(block: DataBlock): Try[String] = Try{ block.utf8String }
    }

    implicit object StringCodecProvider extends FrameCodecProvider[RawString] {
      def provideCodec() = new RawStringCodec
    }
  }
}

trait FrameCodecProvider[P <: Protocol] {
  
  def provideCodec(): FrameCodec[P]

}
