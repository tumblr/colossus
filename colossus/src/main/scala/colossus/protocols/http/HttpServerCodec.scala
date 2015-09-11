package colossus
package protocols.http


import core._
import service._
import parsing._
import DataSize._

class BaseHttpServerCodec[T <: BaseHttpResponse](maxSize: DataSize = 1.MB) extends Codec.ServerCodec[HttpRequest, T] {

  private var parser = HttpRequestParser(maxSize)

  def encode(response: T): DataReader = response.encode()

  def decode(data: DataBuffer): Option[DecodedResult[HttpRequest]] = DecodedResult.static(parser.parse(data))

  def reset(){
    parser = HttpRequestParser(maxSize)
  }
}

class HttpServerCodec(maxSize: DataSize = 1.MB) extends BaseHttpServerCodec[HttpResponse](maxSize)

class StreamingHttpServerCodec(maxSize: DataSize = 1.MB) extends BaseHttpServerCodec[StreamingHttpResponse](maxSize)

