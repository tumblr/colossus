package colossus
package protocols.http



import core._
import service._

class HttpServerCodec extends Codec.ServerCodec[HttpRequest, HttpResponse] {
  private var parser = HttpRequestParser()
  def encode(response: HttpResponse): DataBuffer = response.bytes
  def decode(data: DataBuffer): Option[HttpRequest] = parser.parse(data)
  def reset(){
    parser = HttpRequestParser()
  }
}
