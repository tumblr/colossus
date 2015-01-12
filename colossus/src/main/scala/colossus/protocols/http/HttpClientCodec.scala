package colossus
package protocols.http

import colossus.controller.{FiniteBytePipe, Pipe, InfinitePipe, Sink}
import colossus.core._
import colossus.parsing.DataSize
import colossus.service.Codec.ClientCodec
import colossus.service._

trait HttpClientCodec[T <: HttpResponseHeader, U <: HttpResponseHeader] extends Codec.ClientCodec[HttpRequest, T] {

  def parser : HttpResponseParser[U]

  override def encode(out: HttpRequest): DataBuffer = DataBuffer(out.bytes)

  override def decode(data: DataBuffer): Option[Decoded]

  override def reset(): Unit = { parser.reset() }

}

object HttpClientCodec {

  def static(maxSize : DataSize = HttpResponseParser.DefaultMaxSize) : ClientCodec[HttpRequest, HttpResponse] = {
    new HttpClientCodec[HttpResponse, HttpResponse] {
      val parser: HttpResponseParser[HttpResponse] = HttpResponseParser.static(maxSize)

      override def decode(data: DataBuffer): Option[Decoded] = {
        parser.parse(data).map(DecodedResult.Static(_))
      }
    }
  }

  def streaming(maxSize : DataSize = HttpResponseParser.DefaultMaxSize,
                streamingQueueSize : Int = HttpResponseParser.DefaultQueueSize) : ClientCodec[HttpRequest, StreamingHttpResponse] = {
    new HttpClientCodec[StreamingHttpResponse, StreamingHttpResponseHeaderStub] {
      val parser: HttpResponseParser[StreamingHttpResponseHeaderStub] = HttpResponseParser.streaming(maxSize, streamingQueueSize)

      override def decode(data: DataBuffer): Option[Decoded] = {
        parser.parse(data).map{ x=>
          val rs = x.getEncoding match {
            case TransferEncoding.Chunked => createChunkedReponse(x)
            case _ => createNonChunkedReponse(x)
          }
          DecodedResult.Streamed(rs.response, rs.sink)
        }
      }

      private def createChunkedReponse(headers : StreamingHttpResponseHeaderStub) : ResponseAndSink = {
        val combinator = StreamingHttpChunkPipe(new InfinitePipe[DataBuffer](streamingQueueSize), new InfinitePipe[DataBuffer](streamingQueueSize)) //uergh...this terrifies me
        val res = StreamingHttpResponse(headers.version, headers.code, headers.headers, combinator)
        ResponseAndSink(res, combinator)
      }

      private def createNonChunkedReponse(headers : StreamingHttpResponseHeaderStub) : ResponseAndSink = {
        val sink =  createNonChunkedPipe(headers)
        val res = StreamingHttpResponse(headers.version, headers.code, headers.headers, sink)
        ResponseAndSink(res, sink)
      }

      private def createNonChunkedPipe(headers : StreamingHttpResponseHeaderStub) : Pipe[DataBuffer, DataBuffer] = {
        val length = headers.getContentLength.getOrElse(0)
        new FiniteBytePipe(length, streamingQueueSize)
      }
    }
  }

  private case class ResponseAndSink(response : StreamingHttpResponse, sink : Sink[DataBuffer])
}