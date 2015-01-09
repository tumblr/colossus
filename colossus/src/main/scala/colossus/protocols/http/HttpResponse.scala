package colossus
package protocols.http

import core.DataBuffer

import akka.util.{ByteString, ByteStringBuilder}

case class HttpResponse(version: HttpVersion, code: HttpCode, data: ByteString, headers: Seq[(String, String)] = Nil) {

  import HttpParse._

  def bytes = {
    val builder = new ByteStringBuilder
    builder.sizeHint(100 + data.size)
    builder append version.messageBytes
    builder putByte ' '
    builder append code.headerBytes    
    builder append ByteString("\r\n")
    headers.foreach{case (key, value) =>
      builder putBytes key.getBytes
      builder putBytes ": ".getBytes
      builder putBytes value.getBytes
      builder append NEWLINE
    }
    builder putBytes s"Content-Length: ${data.size}".getBytes
    builder append NEWLINE
    builder append NEWLINE
    builder append data
    DataBuffer(builder.result)
  }

  override def equals(other: Any) = other match {
    case HttpResponse(v, c, d, h) => v == version && c == code && d == data && h.toSet == headers.toSet
    case _ => false
  }

  def withHeader(key: String, value: String) = copy(headers = (key, value) +: headers)
  

  //override def toString = code.toString + ":" + data.utf8String
}

object HttpResponse {
  def apply(version: HttpVersion, code: HttpCode, dataString: String): HttpResponse = HttpResponse(version, code, ByteString(dataString))
}

