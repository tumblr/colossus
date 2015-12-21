package colossus
package protocols.http

import colossus.core._
import colossus.parsing.DataSize
import colossus.service._
import Codec.ClientCodec
import parsing._
import Combinators.Parser
import DataSize._
import encoding._

class HttpClientCodec(maxResponseSize: DataSize = 1.MB, stream: Boolean = false) extends ClientCodec[HttpRequest, HttpResponse] {

  private var parser : Parser[DecodedResult[HttpResponse]] = HttpResponseParser.static(maxResponseSize)

  override def encode(out: HttpRequest) = DataBuffer(out.bytes)

  override def decode(data: DataBuffer): Option[DecodedResult[HttpResponse]] = parser.parse(data)

  override def reset(): Unit = { 
    HttpResponseParser.static(maxResponseSize)
  }

  override def endOfStream() = parser.endOfStream()

}
