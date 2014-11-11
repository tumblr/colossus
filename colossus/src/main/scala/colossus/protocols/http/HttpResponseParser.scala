package colossus
package protocols.http

import core._

import akka.util.ByteString

import colossus.parsing._
import HttpParse._
import Combinators._
import DataSize._


object HttpResponseParser {

  def response: Parser[HttpResponse] = firstLine ~ headers |> {case (version, code) ~ headers =>
    val clength = headers.collectFirst{case (c,v) if (c == HttpHeaders.ContentLength) => v.toInt}.getOrElse(0)
    if (clength > 0) {
      bytes(clength) >> {body => HttpResponse(version, code, body, headers)}
    } else {
      const(HttpResponse(version, code, ByteString(), headers))
    }
  }

  
  protected def firstLine: Parser[(HttpVersion, HttpCode)] = version ~ code >> {case v ~ c => (HttpVersion(v), HttpCodes(c.toInt))}
  
  protected def version = stringUntil(' ')
  
  protected def code = intUntil(' ') <~ stringUntil('\r') <~ byte

}



class HttpResponseParser(maxSize: DataSize = 1.MB) {
  private def parser = Combinators.maxSize(maxSize, HttpResponseParser.response)
  var p = parser
  def parse(data: DataBuffer): Option[HttpResponse] = p.parse(data)
  def reset(){ 
    p = parser
  }
}
