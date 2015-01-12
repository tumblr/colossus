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

  val DefaultQueueSize : Int = 100

  //not totally in love with this extra object allocation, but feel its more straightforward than extractors
  case class ParsedHeaders(headers : Seq[(String, String)]) extends HttpHeaderUtils

  def static(size: DataSize = DefaultMaxSize) : HttpResponseParser[HttpResponse] = {
    new HttpResponseParser[HttpResponse](() => maxSize(size, staticResponse))
  }

  def streaming(size : DataSize = DefaultMaxSize, streamBufferSize : Int = DefaultQueueSize) : HttpResponseParser[StreamingHttpResponseHeaderStub] = {
    new HttpResponseParser[StreamingHttpResponseHeaderStub](() => maxSize(size, streamedResponse(streamBufferSize)))
  }

  def staticResponse: Parser[HttpResponse] = firstLine ~ headers |> {case (version, code) ~ headers =>
    val pHeaders = ParsedHeaders(headers)
    val clength =  pHeaders.getContentLength
    val encoding = pHeaders.getEncoding
    encoding match {
      case TransferEncoding.Identity => clength match {
        case Some(0) | None => const(HttpResponse(version, code, headers, ByteString()))
        case Some(n) => bytes(n) >> {body => HttpResponse(version, code, headers, body)}
      }
      case _  => chunkedBody >> {body => HttpResponse(version, code, headers, body)}
    }
  }

  private def streamedResponse(streamBufferSize : Int) : Parser[StreamingHttpResponseHeaderStub] =
    firstLine ~ headers >> {case (version, code) ~ headers =>
      StreamingHttpResponseHeaderStub(version, code, headers)
    }

  protected def firstLine: Parser[(HttpVersion, HttpCode)] = version ~ code >> {case v ~ c => (HttpVersion(v), HttpCodes(c.toInt))}

  protected def version = stringUntil(' ')

  protected def code = intUntil(' ') <~ stringUntil('\r') <~ byte

}

class HttpResponseParser[T <: HttpResponseHeader](f : () => Parser[T]) {

  private def parser : Parser[T] = f()

  private var current : Parser[T] = parser

  def parse(data : DataBuffer) : Option[T] = current.parse(data)

  def reset() {
    current = parser
  }
}

case class StreamingHttpResponseHeaderStub(version : HttpVersion, code : HttpCode, headers : Seq[(String, String)]) extends HttpResponseHeader