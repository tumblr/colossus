package colossus
package protocols.http


import core._
import controller.Codec
import service.ServiceConfig

class StaticHttpServerCodec(headers: HttpHeaders, serviceConfig: ServiceConfig) extends Codec.Server[Http] {
  private var parser = HttpRequestParser(serviceConfig)

  def encode(response: HttpResponse, buffer: DataOutBuffer): Unit = {
    response.encode(buffer, headers)
  }

  def decode(data: DataBuffer): Option[HttpRequest] = parser.parse(data)

  def reset(): Unit = {
    parser = HttpRequestParser(serviceConfig)
  }

  def endOfStream() = parser.endOfStream()

}

