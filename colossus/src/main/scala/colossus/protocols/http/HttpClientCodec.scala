package colossus
package protocols.http

import colossus.core._
import colossus.parsing.DataSize
import colossus.service._

class HttpClientCodec(maxSize: DataSize = HttpResponseParser.DefaultMaxSize) extends Codec.ClientCodec[HttpRequest, HttpResponse]{

  val parser = new HttpResponseParser(maxSize)

  override def encode(out: HttpRequest): DataBuffer = DataBuffer(out.bytes)

  override def decode(data: DataBuffer): Option[HttpResponse] = parser.parse(data)

  override def reset(): Unit = { parser.reset() }
}
