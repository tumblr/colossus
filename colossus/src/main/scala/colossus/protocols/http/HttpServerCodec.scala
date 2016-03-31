package colossus
package protocols.http


import core._
import service._

class BaseHttpServerCodec[T <: BaseHttpResponse]() extends Codec.ServerCodec[HttpRequest, T] {

  private var parser = HttpRequestParser()

  def encode(response: T): DataReader = response.toReader

  def decode(data: DataBuffer): Option[DecodedResult[HttpRequest]] = DecodedResult.static(parser.parse(data))

  def reset(){
    parser = HttpRequestParser()
  }
}

class HttpServerCodec() extends BaseHttpServerCodec[HttpResponse]

class StreamingHttpServerCodec() extends BaseHttpServerCodec[StreamingHttpResponse]

