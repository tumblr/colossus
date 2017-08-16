package colossus.protocols.http

import HttpParse._
import akka.util.ByteString
import colossus.core.{DataOutBuffer, Encoder}

object HttpResponseHeader {

  //TODO: these are bytestrings, whereas in Head they're strings
  val ContentLength    = ByteString("content-length")
  val TransferEncoding = ByteString("transfer-encoding")

  val DELIM       = ByteString(": ")
  val DELIM_ARRAY = DELIM.toArray
  val SPACE_ARRAY = Array(' '.toByte)

}

trait ResponseFL {
  def version: HttpVersion
  def code: HttpCode

  override def toString = version.toString + " " + code.toString

  override def equals(that: Any): Boolean = that match {
    case t: ResponseFL => this.toString == that.toString
    case _             => false
  }

  override def hashCode = toString.hashCode
}

case class ParsedResponseFL(data: Array[Byte]) extends ResponseFL with LazyParsing {

  protected def parseErrorMessage = "malformed head"

  lazy val codeStart    = fastIndex(data, ' '.toByte) + 1
  lazy val codemsgStart = fastIndex(data, ' '.toByte, codeStart) + 1

  lazy val version: HttpVersion = parsed { HttpVersion(data, 0, codeStart - 1) }
  lazy val code: HttpCode       = parsed { HttpCode((new String(data, codeStart, codemsgStart - codeStart - 1)).toInt) }
}

case class BasicResponseFL(version: HttpVersion, code: HttpCode) extends ResponseFL

case class HttpResponseHead(fl: ResponseFL, headers: HttpHeaders) extends HttpMessageHead {

  def version = fl.version
  def code    = fl.code

  def encode(buffer: DataOutBuffer) {
    buffer.write(version.messageArr)
    buffer.write(HttpResponseHeader.SPACE_ARRAY)
    buffer.write(code.headerArr)
    buffer.write(NEWLINE_ARRAY)
    headers.encode(buffer)
  }

  def withHeader(header: HttpHeader) = copy(headers = headers + header)

}

object HttpResponseHead {

  def apply(version: HttpVersion, code: HttpCode, headers: HttpHeaders): HttpResponseHead = {
    HttpResponseHead(BasicResponseFL(version, code), headers)
  }

  def apply(
      version: HttpVersion,
      code: HttpCode,
      transferEncoding: Option[TransferEncoding],
      contentType: Option[String],
      contentLength: Option[Int],
      connection: Option[Connection],
      extraHeaders: HttpHeaders
  ): HttpResponseHead = {
    apply(version, code, new ParsedHttpHeaders(extraHeaders, transferEncoding, contentType, contentLength, connection))
  }
}

case class HttpResponse(head: HttpResponseHead, body: HttpBody) extends Encoder with HttpMessage[HttpResponseHead] {

  def encode(buffer: DataOutBuffer) = encode(buffer, HttpHeaders.Empty)

  def encode(buffer: DataOutBuffer, extraHeaders: HttpHeaders) {
    head.encode(buffer)
    extraHeaders.encode(buffer)
    //unlike requests, we always encode content-length, even if it's 0
    HttpHeader.encodeContentLength(buffer, body.size)
    buffer.write(N2)
    body.encode(buffer)
  }

  def withHeader(key: String, value: String) = copy(head = head.withHeader(HttpHeader(key, value)))

  def withContentType(contentType: String) = withHeader("Content-Type", contentType)

  def code = head.code

}

/**
  * Converter typeclass for bytestrings.  Default implementations are in package.scala
  */
trait ByteStringLike[T] {

  def toByteString(t: T): ByteString

}

object HttpResponse extends HttpResponseBuilding {

  def initialVersion = HttpVersion.`1.1`

  def apply[T: HttpBodyEncoder](head: HttpResponseHead, body: T): HttpResponse = {
    val encoder = implicitly[HttpBodyEncoder[T]]
    val response = HttpResponse(head, HttpBody(encoder.encode(body)))
    encoder.contentType.fold(response) { ct => response.withContentType(ct) }
  }

  def apply[T: HttpBodyEncoder](version: HttpVersion, code: HttpCode, headers: HttpHeaders, data: T): HttpResponse = {
    HttpResponse(HttpResponseHead(version, code, headers), data)
  }

  def apply(version: HttpVersion, code: HttpCode, headers: HttpHeaders): HttpResponse = {
    HttpResponse(HttpResponseHead(version, code, headers), HttpBody.NoBody)
  }

}

trait HttpResponseBuilding {
  import HttpCodes._

  def initialVersion: HttpVersion

  def respond[T: HttpBodyEncoder](code: HttpCode, data: T, headers: HttpHeaders = HttpHeaders.Empty) = {
    HttpResponse(HttpResponseHead(initialVersion, code, headers), HttpBody(data))
  }

  def ok[T: HttpBodyEncoder](data: T, headers: HttpHeaders = HttpHeaders.Empty) = respond(OK, data, headers)
  def notFound[T: HttpBodyEncoder](data: T, headers: HttpHeaders = HttpHeaders.Empty) =
    respond(NOT_FOUND, data, headers)
  def error[T: HttpBodyEncoder](message: T, headers: HttpHeaders = HttpHeaders.Empty) =
    respond(INTERNAL_SERVER_ERROR, message, headers)
  def badRequest[T: HttpBodyEncoder](message: T, headers: HttpHeaders = HttpHeaders.Empty) =
    respond(BAD_REQUEST, message, headers)
  def unauthorized[T: HttpBodyEncoder](message: T, headers: HttpHeaders = HttpHeaders.Empty) =
    respond(UNAUTHORIZED, message, headers)
  def forbidden[T: HttpBodyEncoder](message: T, headers: HttpHeaders = HttpHeaders.Empty) =
    respond(FORBIDDEN, message, headers)

}
