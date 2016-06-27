package colossus
package protocols.http

import colossus.core._
import colossus.service._
import Codec.ClientCodec
import parsing._
import Combinators.Parser

import controller.StaticCodec

class BaseHttpClientCodec[T <: BaseHttpResponse](parserFactory: () => Parser[DecodedResult[T]]) extends ClientCodec[HttpRequest, T] {

  private var parser : Parser[DecodedResult[T]] = parserFactory()


  override def encode(out: HttpRequest): DataReader = out

  override def decode(data: DataBuffer): Option[DecodedResult[T]] = parser.parse(data)

  override def reset(): Unit = {
    parser = parserFactory()
  }

  override def endOfStream() = parser.endOfStream()

}

class StaticHttpClientCodec extends StaticCodec[Http#ClientEncoding] {

  private var parser : Parser[DecodedResult[HttpResponse]] = HttpResponseParser.static()


  override def encode(out: HttpRequest, buffer: DataOutBuffer) { out.encode(buffer) }

  override def decode(data: DataBuffer): Option[HttpResponse] = parser.parse(data) match {
    case Some(DecodedResult.Static(s)) => Some(s)
    case None => None
    case _ => throw new Exception("NO")
  }

  override def reset(): Unit = {
    parser = HttpResponseParser.static()
  }

  override def endOfStream() = parser.endOfStream() match {
    case Some(DecodedResult.Static(s)) => Some(s)
    case None => None
    case _ => throw new Exception("NO")
  }

}



class HttpClientCodec() extends BaseHttpClientCodec[HttpResponse](() => HttpResponseParser.static())

class StreamingHttpClientCodec(dechunk: Boolean = false)
  extends BaseHttpClientCodec[StreamingHttpResponse](() => HttpResponseParser.stream(dechunk))

