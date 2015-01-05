package colossus
package protocols.http


import akka.util.ByteStringBuilder
import colossus.controller.{DualSource, Source}
import core._
import service._

//TODO: add chunked transfer support
class HttpServerCodec[T <: HttpResponseHeader] extends Codec.ServerCodec[HttpRequest, T] {

  private var parser = HttpRequestParser()

  def encode(response: T): DataReader = HttpResponseDataReader(response)

  def decode(data: DataBuffer): Option[DecodedResult[HttpRequest]] = DecodedResult.static(parser.parse(data))

  def reset(){
    parser = HttpRequestParser()
  }
}

object HttpServerCodec {
  def static : HttpServerCodec[HttpResponse] = new HttpServerCodec[HttpResponse]
  def streaming : HttpServerCodec[StreamingHttpResponse] = new HttpServerCodec[StreamingHttpResponse]
}


trait HttpResponseDataReaderBuilder {

  import colossus.protocols.http.HttpParse._

  def generatedHeaders : Seq[String]

  def appendHeaderBytes(response : HttpResponseHeader, builder : ByteStringBuilder) {
    builder append response.version.messageBytes
    builder putByte ' '
    builder append response.code.headerBytes
    builder append NEWLINE
    response.headers.foreach{case (key, value) =>
      if(!generatedHeaders.contains(key.toLowerCase)) {
        builder putBytes key.getBytes
        builder putBytes ": ".getBytes
        builder putBytes value.getBytes
        builder append NEWLINE
      }
    }
  }
}

object HttpResponseDataReader {

  def apply(response: HttpResponseHeader): DataReader = {
    response match {
      case a: StreamingHttpResponse => StreamedResponseBuilder(a)
      case b: HttpResponse => StaticResponseBuilder(b)
    }
  }
}

object StaticResponseBuilder extends HttpResponseDataReaderBuilder{

  import colossus.protocols.http.HttpParse._

  val generatedHeaders = Seq("content-length")

  def apply(response : HttpResponse) : DataBuffer = {
    val builder = new ByteStringBuilder
    builder.sizeHint(100 + response.data.size)
    appendHeaderBytes(response, builder)
    builder putBytes s"Content-Length: ${response.data.size}".getBytes
    builder append NEWLINE
    builder append NEWLINE
    builder append response.data
    DataBuffer(builder.result())
  }
}

object StreamedResponseBuilder extends HttpResponseDataReaderBuilder {

  import colossus.protocols.http.HttpParse._

  val generatedHeaders = Seq.empty

  def apply(response : StreamingHttpResponse) : DataStream = {
    val builder = new ByteStringBuilder
    builder.sizeHint(100)
    appendHeaderBytes(response, builder)
    builder append NEWLINE
    builder append NEWLINE

    val headerSource: Source[DataBuffer] = Source.one(DataBuffer(builder.result()))
    DataStream(new DualSource[DataBuffer](headerSource, response.stream))
  }

}