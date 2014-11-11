package colossus
package protocols.http

import akka.util.ByteString

import parsing._
import Combinators._

object HttpParse {

  val NEWLINE = ByteString("\r\n")

  //common parsers
  def headers: Parser[List[(String, String)]]    = repeatUntil(header, '\r') <~ byte
  def header: Parser[(String, String)]     = {
    stringUntil(':', toLower = true, allowWhiteSpace = false) ~ 
    stringUntil('\r', allowWhiteSpace = true, ltrim = true) <~ 
    byte >> {case key ~ value => (key, value)}
  }

}

