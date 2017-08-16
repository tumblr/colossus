package colossus
package protocols.http


import core._
import controller.Codec
import parsing.DataSize

class StaticHttpServerCodec(headers: HttpHeaders, maxRequestSize: DataSize) extends Codec.Server[Http] {
  private var parser = HttpRequestParser(maxRequestSize)

  def encode(response: HttpResponse, buffer: DataOutBuffer): Unit = {
    response.encode(buffer, headers)
  }

  def decode(data: DataBuffer): Option[HttpRequest] = parser.parse(data)

  def reset(): Unit = {
    parser = HttpRequestParser(maxRequestSize)
  }

  def endOfStream() = parser.endOfStream()

}

