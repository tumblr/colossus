package colossus
package controller

import core._
import service.Codec

trait TestInput {
  def sink: Sink[DataBuffer]
}

case class TestInputImpl(data: FiniteBytePipe) extends StreamMessage with TestInput{
  def source = data
  def sink = data
}
  
case class TestOutput(data: Sink[DataBuffer])


class TestCodec extends Codec[TestOutput, TestInput]{    
  import parsing.Combinators._
  val parser = intUntil('\r') <~ byte >> {num => TestInputImpl(new FiniteBytePipe(num, 10))}

  def decode(data: DataBuffer): Option[TestInput] = parser.parse(data)

  //TODO: need to add support for pipe combinators, eg A ++ B
  def encode(out: TestOutput) = DataStream(out.data)

  def reset(){}
}
