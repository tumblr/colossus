package colossus
package protocols.http


import core._
import service._
import controller.StaticCodec

class StaticHttpServerCodec(headers: HttpHeaders) extends StaticCodec[Http#ServerEncoding] {
  private var parser = HttpRequestParser()

  def encode(response: HttpResponse, buffer: DataOutBuffer){ response.encode(buffer, headers) }

  def decode(data: DataBuffer): Option[HttpRequest] = parser.parse(data)

  def reset(){
    parser = HttpRequestParser()
  }

  def endOfStream() = parser.endOfStream()

}

