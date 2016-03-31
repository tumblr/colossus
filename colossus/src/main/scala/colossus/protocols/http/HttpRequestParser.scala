package colossus
package protocols.http

import core.DataOutBuffer
import parsing._
import Combinators._

object HttpRequestParser {
  import HttpParse._
  
  def apply() = httpRequest

  //TODO : don't parse body as a bytestring
  protected def httpRequest: Parser[HttpRequest] = httpHead |> {case HeadResult(head, contentLength, transferEncoding) =>
    transferEncoding match {
      case None | Some("identity") => contentLength match {
        case Some(0) | None => const(HttpRequest(head, HttpBody.NoBody))
        case Some(n) => bytes(n) >> {body => HttpRequest(head, HttpBody(body))}
      }
      case Some(other)  => chunkedBody >> {body => HttpRequest(head, HttpBody(body))}
    }
  }

  protected def httpHead = firstLine ~ headers >> {case fl ~ headersBuilder =>
    HeadResult(
      HttpRequestHead(fl, headersBuilder.buildHeaders),
      headersBuilder.contentLength,
      headersBuilder.transferEncoding
    )
  }

  def firstLine = line(ParsedFL.apply, true)
  
}


case class ParsedFL(data: Array[Byte]) extends FirstLine with LazyParsing {

  protected def parseErrorMessage = "Malformed head"


  def encode(out: DataOutBuffer) {
    out.write(data)
  }
  lazy val method     = parsed {HttpMethod(data)}

  //private lazy val pathStart  = fastIndex(data, ' '.toByte, 3) + 1
  private def pathStart = method.encodedSize + 1
  private def pathLength = data.length - 11 - pathStart //assumes the line ends with " HTTP/x/x\r\n", which it always should

  lazy val path       = parsed { new String(data, pathStart, pathLength) }
  lazy val version    = parsed {
    val vstart = data.length - 10
    HttpVersion(data, vstart, data.length - vstart - 2)
  }
}





