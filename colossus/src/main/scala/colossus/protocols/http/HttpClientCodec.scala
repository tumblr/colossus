package colossus
package protocols.http

import colossus.controller.Sink
import colossus.core._
import colossus.parsing.DataSize
import colossus.service.Codec.ClientCodec
import colossus.service._

trait HttpClientCodec[T <: HttpResponseHeader] extends Codec.ClientCodec[HttpRequest, T] {

  def parser : HttpResponseParser[T]

  override def encode(out: HttpRequest): DataBuffer = DataBuffer(out.bytes)

  override def decode(data: DataBuffer): Option[Decoded]

  override def reset(): Unit = { parser.reset() }

}

object HttpClientCodec {

  def static(maxSize : DataSize = HttpResponseParser.DefaultMaxSize) : ClientCodec[HttpRequest, HttpResponse] = {
    new HttpClientCodec[HttpResponse] {
      val parser: HttpResponseParser[HttpResponse] = HttpResponseParser.static(maxSize)

      override def decode(data: DataBuffer): Option[Decoded] = {
        parser.parse(data).map(DecodedResult.Static(_))
      }
    }
  }

  def streaming(maxSize : DataSize = HttpResponseParser.DefaultMaxSize,
                streamingQueueSize : Int = HttpResponseParser.DefaultQueueSize) : HttpClientCodec[StreamingHttpResponse] = {
    new HttpClientCodec[StreamingHttpResponse] {
      val parser: HttpResponseParser[StreamingHttpResponse] = HttpResponseParser.streaming(maxSize, streamingQueueSize)

      override def decode(data: DataBuffer): Option[Decoded] = {
        parser.parse(data).map(x => DecodedResult.Streamed(x, x.stream.asInstanceOf[Sink[DataBuffer]]))
      }
    }
  }
}