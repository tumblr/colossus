package colossus
package protocols.http


import core._
import service._
import parsing._
import DataSize._

class HttpServerCodec(maxSize: DataSize = 1.MB) extends Codec.ServerCodec[HttpRequest, HttpResponse]{
  private var parser = HttpRequestParser(maxSize)

  def encode(response: HttpResponse): DataReader = response.encode()

  def decode(data: DataBuffer): Option[DecodedResult[HttpRequest]] = DecodedResult.static(parser.parse(data))

  def reset(){
    parser = HttpRequestParser(maxSize)
  }
}


