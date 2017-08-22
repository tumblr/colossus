package colossus.protocols.http

import akka.util.ByteString
import colossus.parsing.Combinators._
import colossus.parsing.{ParseException, Zero}

object HttpParse {

  val NEWLINE       = ByteString("\r\n")
  val NEWLINE_ARRAY = NEWLINE.toArray
  val N2            = (NEWLINE ++ NEWLINE).toArray

  def chunkedBody: Parser[HttpBody] = chunkedBodyBuilder(new FastArrayBuilder(100, false))

  implicit val z: Zero[EncodedHttpHeader]                                        = HttpHeader.FPHZero
  def header: Parser[EncodedHttpHeader]                                          = line(HttpHeader.apply, true)
  def folder(header: EncodedHttpHeader, builder: HeadersBuilder): HeadersBuilder = builder.add(header)
  def headers: Parser[HeadersBuilder]                                            = foldZero(header, new HeadersBuilder)(folder)

  private def chunkedBodyBuilder(builder: FastArrayBuilder): Parser[HttpBody] = intUntil('\r', 16) <~ byte |> {
    case 0 =>
      bytes(2) >> { _ =>
        new HttpBody(builder.complete())
      }
    case n =>
      bytes(n.toInt) <~ bytes(2) |> { bytes =>
        builder.write(bytes); chunkedBodyBuilder(builder)
      }
  }

  class HeadersBuilder {

    private var contentLength: Option[Int]                 = None
    private var contentType: Option[String]                = None
    private var transferEncoding: Option[TransferEncoding] = None
    private var connection: Option[Connection]             = None

    private val build = new java.util.LinkedList[EncodedHttpHeader]()

    def add(header: EncodedHttpHeader): HeadersBuilder = {
      build.add(header)
      if (contentLength.isEmpty && header.matches(HttpHeaders.ContentLength)) {
        contentLength = Some(header.value.toInt)
      }
      if (contentType.isEmpty && header.matches(HttpHeaders.ContentType)) {
        contentType = Some(header.value)
      }
      if (transferEncoding.isEmpty && header.matches(HttpHeaders.TransferEncoding)) {
        transferEncoding = Some(TransferEncoding(header.value))
      }
      if (connection.isEmpty && header.matches(HttpHeaders.Connection)) {
        connection = Some(Connection(header.value))
      }

      this
    }

    def buildHeaders: ParsedHttpHeaders = {
      new ParsedHttpHeaders(
        build.asInstanceOf[java.util.List[HttpHeader]], //silly invariant java collections :/
        transferEncoding,
        contentType,
        contentLength,
        connection
      )
    }

  }
}

trait LazyParsing {

  protected def parseErrorMessage: String

  def parsed[T](op: => T): T =
    try {
      op
    } catch {
      case p: ParseException => throw p
      case other: Throwable  => throw new ParseException(parseErrorMessage + s": $other")
    }

  def fastIndex(data: Array[Byte], byte: Byte, start: Int = 0) = {
    var pos = start
    while (pos < data.length && data(pos) != byte) { pos += 1 }
    if (pos >= data.length) -1 else pos
  }

}
