package colossus
package protocols.http

import core._
import service._

class HttpClientCodec extends Codec.ClientCodec[HttpRequest, HttpResponse]{

  val parser = new HttpResponseParser

  override def encode(out: HttpRequest): DataBuffer = DataBuffer(out.bytes)

  override def decode(data: DataBuffer): Option[HttpResponse] = parser.parse(data)

  override def reset(): Unit = { parser.reset() }
}
