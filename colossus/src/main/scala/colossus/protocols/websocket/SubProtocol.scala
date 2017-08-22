package colossus.protocols.websocket

import colossus.controller.Encoding
import colossus.core.DataBlock

import scala.util.Try

trait FrameCodec[E <: Encoding] {

  def decode(data: DataBlock): Try[E#Input]
  def encode(output: E#Output): DataBlock
}

object subprotocols {

  object rawstring {

    trait RawString extends Encoding {
      type Input  = String
      type Output = String
    }

    class RawStringCodec extends FrameCodec[RawString] {
      def encode(str: String): DataBlock        = DataBlock(str)
      def decode(block: DataBlock): Try[String] = Try { block.utf8String }
    }

    implicit object StringCodecProvider extends FrameCodecProvider[RawString] {
      def provideCodec() = new RawStringCodec
    }
  }
}

trait FrameCodecProvider[E <: Encoding] {

  def provideCodec(): FrameCodec[E]

}
