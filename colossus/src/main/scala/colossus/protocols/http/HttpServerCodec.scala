package colossus
package protocols.http


import core._
import service._
import controller.StaticCodec

class BaseHttpServerCodec[T <: BaseHttpResponse]() extends Codec.ServerCodec[HttpRequest, T] {

  private var parser = HttpRequestParser()

  def encode(response: T): DataReader = response.toReader

  def decode(data: DataBuffer): Option[DecodedResult[HttpRequest]] = DecodedResult.static(parser.parse(data))

  def reset(){
    parser = HttpRequestParser()
  }
}

class StaticHttpServerCodec(headers: HttpHeaders) extends StaticCodec[Http] {
  private var parser = HttpRequestParser()

  def encode(response: HttpResponse, buffer: DataOutBuffer){ response.encode(buffer, headers) }

  def decode(data: DataBuffer): Option[HttpRequest] = parser.parse(data)

  def reset(){
    parser = HttpRequestParser()
  }

}

class HttpServerCodec() extends BaseHttpServerCodec[HttpResponse]

class StreamingHttpServerCodec() extends BaseHttpServerCodec[StreamingHttpResponse]

