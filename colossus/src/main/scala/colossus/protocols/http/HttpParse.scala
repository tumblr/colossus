package colossus
package protocols.http

import akka.util.{ByteString, ByteStringBuilder}

import parsing._
import Combinators._

object HttpParse {

  val NEWLINE = ByteString("\r\n")
  val NEWLINE_ARRAY = NEWLINE.toArray
  val N2 = (NEWLINE ++ NEWLINE).toArray

  def chunkedBody: Parser[HttpBody] = chunkedBodyBuilder(new FastArrayBuilder(100, false))

  implicit val z: Zero[EncodedHttpHeader] = HttpHeader.FPHZero
  def header: Parser[EncodedHttpHeader] = line(HttpHeader.apply, true)
  def folder(header: EncodedHttpHeader, builder: HeadersBuilder): HeadersBuilder = builder.add(header)
  def headers: Parser[HeadersBuilder] = foldZero(header, new HeadersBuilder)(folder)

  private def chunkedBodyBuilder(builder: FastArrayBuilder): Parser[HttpBody] = intUntil('\r', 16) <~ byte |> {
    case 0 => bytes(2) >> {_ => new HttpBody(builder.complete())}
    case n => bytes(n.toInt) <~ bytes(2) |> {bytes => builder.write(bytes);chunkedBodyBuilder(builder)}
  }


  class HeadersBuilder {

    private var cl: Option[Int]     = None
    private var te: Option[TransferEncoding]  = None
    private var co: Option[Connection]  = None

    def contentLength = cl
    def transferEncoding = te
    def connection = co

    private val build = new java.util.LinkedList[EncodedHttpHeader]()

    def add(header: EncodedHttpHeader): HeadersBuilder = {
      build.add(header)
      if (cl.isEmpty && header.matches(HttpHeaders.ContentLength)) {
        cl = Some(header.value.toInt)
      }
      if (te.isEmpty && header.matches(HttpHeaders.TransferEncoding)) {
        te = Some(TransferEncoding(header.value))
      }
      if (co.isEmpty && header.matches(HttpHeaders.Connection)) {
        co = Some(Connection(header.value))
      }

      this
    }


    def buildHeaders: ParsedHttpHeaders = {
      new ParsedHttpHeaders(build.asInstanceOf[java.util.List[HttpHeader]], te, cl, co) //silly invariant java collections :/
    }

  }
}


trait LazyParsing {

  protected def parseErrorMessage: String

  def parsed[T](op: => T): T = try {
    op
  } catch {
    case p: ParseException => throw p
    case other : Throwable => throw new ParseException(parseErrorMessage + s": $other")
  }

  def fastIndex(data: Array[Byte], byte: Byte, start: Int = 0) = {
    var pos = start
    while (pos < data.length && data(pos) != byte) { pos += 1 }
    if (pos >= data.length) -1 else pos
  }

}

