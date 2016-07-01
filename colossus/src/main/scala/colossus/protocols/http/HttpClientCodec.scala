package colossus
package protocols.http

import colossus.core._
import colossus.service._
import parsing._
import Combinators.Parser

import controller.StaticCodec

class StaticHttpClientCodec extends StaticCodec[Http#ClientEncoding] {

  private var parser : Parser[HttpResponse] = HttpResponseParser.static()


  override def encode(out: HttpRequest, buffer: DataOutBuffer) { out.encode(buffer) }

  override def decode(data: DataBuffer): Option[HttpResponse] = parser.parse(data) 

  override def reset(): Unit = {
    parser = HttpResponseParser.static()
  }

  override def endOfStream() = parser.endOfStream() 

}

