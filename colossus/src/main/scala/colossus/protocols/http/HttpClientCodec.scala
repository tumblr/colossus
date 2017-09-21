package colossus.protocols.http

import colossus.controller.{Codec, Encoding}
import colossus.core._
import colossus.parsing.Combinators.Parser

class StaticHttpClientCodec extends Codec[Encoding.Client[Http]] {

  private var parser: Parser[HttpResponse] = HttpResponseParser.static()

  override def encode(out: HttpRequest, buffer: DataOutBuffer) { out.encode(buffer) }

  override def decode(data: DataBuffer): Option[HttpResponse] = parser.parse(data)

  override def reset(): Unit = {
    parser = HttpResponseParser.static()
  }

  override def endOfStream() = parser.endOfStream()

}
