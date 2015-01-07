package colossus
package protocols.http

import core._

import akka.util.ByteString

import colossus.parsing._
import HttpParse._
import Combinators._
import DataSize._


object HttpResponseParser {


  val DefaultMaxSize: DataSize = 1.MB

  def apply(size: DataSize = DefaultMaxSize) = maxSize(size, response)

  def response: Parser[HttpResponse] = firstLine ~ headers |> {case (version, code) ~ headers =>
    val clength = headers.collectFirst{case (HttpHeaders.ContentLength,v)  => v.toInt}
    val encoding = headers.collectFirst{case (HttpHeaders.TransferEncoding,v)  => v.toLowerCase}
    encoding match { 
      case None | Some("identity") => clength match {
        case Some(0) | None => const(HttpResponse(version, code, ByteString(), headers))
        case Some(n) => bytes(n) >> {body => HttpResponse(version, code, body, headers)}
      }
      case Some(other)  => chunkedBody >> {body => HttpResponse(version, code, body, headers)}
    } 
  }

  
  protected def firstLine: Parser[(HttpVersion, HttpCode)] = version ~ code >> {case v ~ c => (HttpVersion(v), HttpCodes(c.toInt))}
  
  protected def version = stringUntil(' ')
  
  protected def code = intUntil(' ') <~ stringUntil('\r') <~ byte

}



class HttpResponseParser(maxSize: DataSize = 1.MB) {
  private def parser = HttpResponseParser(maxSize)
  var p = parser
  def parse(data: DataBuffer): Option[HttpResponse] = p.parse(data)
  def reset(){ 
    p = parser
  }
}
