package colossus
package protocols.http

import akka.util.{ByteString, ByteStringBuilder}
import colossus.controller.Pipe
import colossus.core.DataBuffer

import parsing._
import Combinators._

object HttpParse {

  val NEWLINE = ByteString("\r\n")

  //common parsers
  def headers: Parser[Vector[(String, String)]]    = repeatUntil(header, '\r') <~ byte
  def header: Parser[(String, String)]     = {
    stringUntil(':', toLower = true, allowWhiteSpace = false) ~ 
    stringUntil('\r', allowWhiteSpace = true, ltrim = true) <~ 
    byte >> {case key ~ value => (key, value)}
  }

  def chunkedBody: Parser[ByteString] = chunkedBodyBuilder(new ByteStringBuilder)

  private def chunkedBodyBuilder(builder: ByteStringBuilder): Parser[ByteString] = intUntil('\r', 16) <~ byte |> {
    case 0 => bytes(2) >> {_ => builder.result}
    case n => bytes(n.toInt) <~ bytes(2) |> {bytes => chunkedBodyBuilder(builder.append(bytes))}
  }
}

