package colossus.protocols.http

import colossus.core.DataOutBuffer
import colossus.parsing.Combinators._
import colossus.parsing.DataSize
import colossus.parsing.DataSize._

object HttpRequestParser {
  import HttpParse._

  def apply(maxRequestSize: DataSize) = httpRequest(maxRequestSize)

  protected def httpRequest(maxRequestSize: DataSize): Parser[HttpRequest] = httpHead |> { head =>
    head.headers.transferEncoding match {
      case TransferEncoding.Identity =>
        head.headers.contentLength match {
          case Some(0) | None => const(HttpRequest(head, HttpBody.NoBody))
          case Some(n) => {
            bytes(n, maxRequestSize, 1.KB) >> { body =>
              HttpRequest(head, new HttpBody(body))
            }
          }
        }
      case _ =>
        chunkedBody >> { body =>
          HttpRequest(head, HttpBody(body))
        }
    }
  }

  def httpHead = firstLine ~ headers >> {
    case fl ~ headersBuilder =>
      ParsedHead(fl, headersBuilder.buildHeaders)
  }

  def firstLine = line(ParsedFL.apply, true)

}

case class ParsedFL(data: Array[Byte]) extends FirstLine with LazyParsing {

  protected def parseErrorMessage = "Malformed head"

  def encode(out: DataOutBuffer) {
    out.write(data)
  }
  lazy val method = parsed { HttpMethod(data) }

  //private lazy val pathStart  = fastIndex(data, ' '.toByte, 3) + 1
  private def pathStart = method.encodedSize + 1
  private def pathLength =
    data.length - 11 - pathStart //assumes the line ends with " HTTP/x/x\r\n", which it always should

  lazy val path = parsed { new String(data, pathStart, pathLength) }
  lazy val version = parsed {
    val vstart = data.length - 10
    HttpVersion(data, vstart, data.length - vstart - 2)
  }
}
