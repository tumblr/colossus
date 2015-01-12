package colossus
package protocols.http

import colossus.controller.{Pipe, InfinitePipe, FiniteBytePipe}
import core._

import akka.util.ByteString

import colossus.parsing._
import HttpParse._
import Combinators._
import DataSize._


object HttpResponseParser {

  import HttpHeaderUtils._

  val DefaultMaxSize: DataSize = 1.MB

  val DefaultQueueSize : Int = 100

  def static(size: DataSize = DefaultMaxSize) : HttpResponseParser[HttpResponse] = {
    new HttpResponseParser[HttpResponse](() => maxSize(size, staticResponse))
  }

  def streaming(size : DataSize = DefaultMaxSize, streamBufferSize : Int = DefaultQueueSize) : HttpResponseParser[StreamingHttpResponse] = {
    new HttpResponseParser[StreamingHttpResponse](() => maxSize(size, streamedResponse(streamBufferSize)))
  }

  def staticResponse: Parser[HttpResponse] = firstLine ~ headers |> {case (version, code) ~ headers =>
    val clength =  getContentLength(headers)
    val encoding = getHeader(headers, HttpHeaders.TransferEncoding)
    encoding match {
      case None | Some("identity") => clength match {
        case Some(0) | None => const(HttpResponse(version, code, headers, ByteString()))
        case Some(n) => bytes(n) >> {body => HttpResponse(version, code, headers, body)}
      }
      case Some(other)  => chunkedBody >> {body => HttpResponse(version, code, headers, body)}
    }
  }

  //TODO:Chunked
  //what do i need to do here?  i need to build a pipe, which reads in chunks
  //and strips the chunkiness out it and emits the raw chunk data
  private def streamedResponse(streamBufferSize : Int) : Parser[StreamingHttpResponse] =
    firstLine ~ headers >> {case (version, code) ~ headers =>
      val clength = getContentLength(headers)
      val encoding = getHeader(headers, HttpHeaders.TransferEncoding)
      encoding match {
        case None | Some("identity") => {
          val p : Pipe[DataBuffer] = clength.fold(new InfinitePipe[DataBuffer](streamBufferSize) : Pipe[DataBuffer])(x => new FiniteBytePipe(x, streamBufferSize))
          StreamingHttpResponse(version, code, headers, p)
        }
        case Some(other)  => StreamingHttpResponse(version, code, headers, new InfinitePipe[DataBuffer](streamBufferSize))  //FOR NOW
      }
    }


  protected def getContentLength(headers : Seq[(String, String)]) : Option[Int] = {
    getHeader(headers, HttpHeaders.ContentLength).map(_.toInt)
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
