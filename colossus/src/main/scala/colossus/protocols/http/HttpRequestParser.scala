package colossus
package protocols.http

import akka.util.ByteString

import core.DataBuffer
import parsing._
import DataSize._
import HttpParse._

object HttpRequestParser {
  import Combinators._

  val DefaultMaxSize: DataSize = 1.MB

  def apply(size: DataSize = DefaultMaxSize) = maxSize(size, httpRequest)

  protected def httpRequest: Parser[HttpRequest] = httpHead |> {head => 
    head.singleHeader(HttpHeaders.TransferEncoding) match { 
      case None | Some("identity") => head.contentLength match {
        case Some(0) | None => const(HttpRequest(head, None))
        case Some(n) => bytes(n) >> {body => HttpRequest(head, Some(body))}
      }
      case Some(other)  => chunkedBody >> {body => HttpRequest(head, Some(body))}
    } 
  }

  protected def httpHead = firstline ~ headers >> {case method ~ path ~ version ~ headers => 
    HttpHead(HttpMethod(method), path, HttpVersion(version), headers)
  }
  
  protected def firstline  = stringUntil(' ', minSize = Some(1), allowWhiteSpace = false) ~ 
    stringUntil(' ', minSize = Some(1), allowWhiteSpace = false) ~ 
    stringUntil('\r', minSize = Some(1), allowWhiteSpace = false) <~ byte
}


