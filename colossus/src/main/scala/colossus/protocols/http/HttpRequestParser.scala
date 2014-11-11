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
    if (head.contentLength > 0) {
      bytes(head.contentLength) >> {body => HttpRequest(head, Some(body))}
    } else {
      const(HttpRequest(head, None))
    }
  }

  protected def httpHead = firstline ~ headers >> {case method ~ path ~ version ~ headers => 
    HttpHead(HttpMethod(method), path, HttpVersion(version), headers)
  }
  
  protected def firstline  = stringUntil(' ', minSize = Some(1), allowWhiteSpace = false) ~ 
    stringUntil(' ', minSize = Some(1), allowWhiteSpace = false) ~ 
    stringUntil('\r', minSize = Some(1), allowWhiteSpace = false) <~ byte
}


