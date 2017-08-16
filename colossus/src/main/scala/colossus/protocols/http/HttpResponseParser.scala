package colossus.protocols.http

import akka.util.ByteStringBuilder
import colossus.parsing._
import HttpParse._
import Combinators._
import colossus.core.DataBuffer

object HttpResponseParser {

  def static(): Parser[HttpResponse] = staticBody(true)

  protected def staticBody(dechunk: Boolean): Parser[HttpResponse] = head |> { parsedHead =>
    parsedHead.headers.transferEncoding match {
      case TransferEncoding.Identity =>
        parsedHead.headers.contentLength match {
          case Some(0) => const(HttpResponse(parsedHead, HttpBody.NoBody))
          case Some(n) =>
            bytes(n) >> { body =>
              HttpResponse(parsedHead, new HttpBody(body))
            }
          case None if (parsedHead.code.isInstanceOf[NoBodyCode]) => const(HttpResponse(parsedHead, HttpBody.NoBody))
          case None =>
            bytesUntilEOS >> { body =>
              HttpResponse(parsedHead, HttpBody(body))
            }
        }
      case _ =>
        chunkedBody >> { body =>
          HttpResponse(parsedHead, HttpBody(body))
        }
    }
  }

  def head: Parser[HttpResponseHead] = firstLine ~ headers >> {
    case fl ~ hbuilder =>
      HttpResponseHead(fl, hbuilder.buildHeaders)
  }

  protected def firstLine = line(true) >> ParsedResponseFL.apply

}

object HttpChunk {

  def wrap(data: DataBuffer): DataBuffer = {
    val builder = new ByteStringBuilder
    builder.sizeHint(data.size + 25)
    builder.putBytes(data.size.toHexString.getBytes)
    builder.append(HttpParse.NEWLINE)
    builder.putBytes(data.takeAll)
    builder.append(HttpParse.NEWLINE)
    DataBuffer(builder.result)
  }
}
