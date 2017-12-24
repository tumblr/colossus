package colossus.protocols.http

import colossus.controller.Codec
import colossus.core.{DataBuffer, DataOutBuffer}
import colossus.util.DataSize

class HttpServerCodec(headers: HttpHeaders, maxRequestSize: DataSize) extends Codec.Server[Http] {
  private var parser = HttpRequestParser(maxRequestSize)

  def encode(response: HttpResponse, buffer: DataOutBuffer): Unit = {
    response.encode(buffer, headers)
  }

  def decode(data: DataBuffer): Option[HttpRequest] = parser.parse(data)

  def reset(): Unit = {
    parser = HttpRequestParser(maxRequestSize)
  }

  def endOfStream(): Option[HttpRequest] = parser.endOfStream()

}
