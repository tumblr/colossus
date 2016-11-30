package colossus
package protocols.http

import akka.util.ByteString
import core.{DataOutBuffer, Encoder}

trait FirstLine extends Encoder {
  def method : HttpMethod
  def path : String
  def version : HttpVersion

  override def toString = s"$method $path $version"

  override def equals(that: Any): Boolean = that match {
    case that : FirstLine => this.toString == that.toString
    case _ => false
  }

  override def hashCode = toString.hashCode

}

case class BuildFL(method: HttpMethod, path: String, version: HttpVersion) extends FirstLine {

  def encode(buffer: DataOutBuffer) {
    buffer write method.bytes
    buffer write ' '
    buffer write path.getBytes("UTF-8")
    buffer write ' '
    buffer write version.messageArr
    buffer write HttpParse.NEWLINE_ARRAY
  }
}

case class BuiltHead(firstLine: BuildFL, headers: HttpHeaders) extends HttpRequestHead

case class ParsedHead(firstLine: ParsedFL, headers: HttpHeaders) extends HttpRequestHead

trait HttpRequestHead extends Encoder {
  def firstLine: FirstLine
  def headers: HttpHeaders

  def copy(
    method  : HttpMethod  = firstLine.method,
    path    : String      = firstLine.path,
    version : HttpVersion = firstLine.version,
    headers : HttpHeaders = headers
  ): HttpRequestHead = {
    BuiltHead(BuildFL(method, path, version), headers)
  }

  override def hashCode = firstLine.hashCode + headers.hashCode

  override def equals(that: Any) = that match {
    case that : HttpRequestHead => this.firstLine == that.firstLine && this.headers == that.headers
    case _ => false
  }

  lazy val method = firstLine.method
  lazy val url = firstLine.path
  lazy val version = firstLine.version

  lazy val (path, query) = {
    val pieces = url.split("\\?",2)
    (pieces(0), if (pieces.size > 1) Some(pieces(1)) else None)
  }

  lazy val parameters: QueryParameters = query.map { qstring =>
    def decode(s: String) = java.net.URLDecoder.decode(s, "UTF-8")
    var build = Vector[(String, String)]()
    var remain = qstring
    while (remain != "") {
      val keyval = remain.split("&", 2)
      val splitKV = keyval(0).split("=", 2)
      val key = decode(splitKV(0))
      val value = if (splitKV.size > 1) decode(splitKV(1)) else ""
      build = build :+ (key -> value)
      remain = if (keyval.size > 1) keyval(1) else ""
    }
    QueryParameters(build)
  } getOrElse QueryParameters(Vector())

  def withHeader(header: String, value: String): HttpRequestHead = {
    copy(headers = headers + (header -> value))
  }

  lazy val cookies: Seq[Cookie] = headers.allValues(HttpHeaders.CookieHeader).flatMap{Cookie.parseHeader}

  def encode(buffer: core.DataOutBuffer) {
    firstLine encode buffer
    headers encode buffer
  }

  def persistConnection: Boolean = {
    (version, headers.connection) match {
      case (HttpVersion.`1.1`, Some(Connection.Close)) => false
      case (HttpVersion.`1.1`, _) => true
      case (HttpVersion.`1.0`, Some(Connection.KeepAlive)) => true
      case (HttpVersion.`1.0`, _) => false
    }
  }

}

object HttpRequestHead {

  def apply(method: HttpMethod, url: String, version: HttpVersion, headers: HttpHeaders): HttpRequestHead = {
    BuiltHead(BuildFL(method, url, version), headers)
  }

}

case class HttpRequest(head: HttpRequestHead, body: HttpBody) extends Encoder with HttpRequestBuilding[HttpRequest] {
  import head._
  import HttpCodes._

  protected def current = this

  protected def next(req: HttpRequest) = req

  def respond[T : HttpBodyEncoder](code: HttpCode, data: T, headers: HttpHeaders = HttpHeaders.Empty) = {
    HttpResponse(HttpResponseHead(version, code, headers), HttpBody(data))
  }

  def ok[T : HttpBodyEncoder](data: T, headers: HttpHeaders = HttpHeaders.Empty)              = respond(OK, data, headers)
  def notFound[T : HttpBodyEncoder](data: T, headers: HttpHeaders = HttpHeaders.Empty)        = respond(NOT_FOUND, data, headers)
  def error[T : HttpBodyEncoder](message: T, headers: HttpHeaders = HttpHeaders.Empty)        = respond(INTERNAL_SERVER_ERROR, message, headers)
  def badRequest[T : HttpBodyEncoder](message: T, headers: HttpHeaders = HttpHeaders.Empty)   = respond(BAD_REQUEST, message, headers)
  def unauthorized[T : HttpBodyEncoder](message: T, headers: HttpHeaders = HttpHeaders.Empty) = respond(UNAUTHORIZED, message, headers)
  def forbidden[T : HttpBodyEncoder](message: T, headers: HttpHeaders = HttpHeaders.Empty)    = respond(FORBIDDEN, message, headers)

  def encode(buffer: core.DataOutBuffer) {
    head encode buffer
    if (body.size == 0) {
      buffer write HttpParse.NEWLINE_ARRAY
    } else {
      HttpHeader.encodeContentLength(buffer, body.size)
      buffer write HttpParse.N2
      body encode buffer
    }

  }

}

trait HttpRequestBuilding[T] {

  protected def current: HttpRequest

  protected def next(req: HttpRequest): T

  protected def transformHead(f: HttpRequestHead => HttpRequestHead): T = next(current.copy(head = f(current.head)))


  def withHeader(header: HttpHeader): T = transformHead(_.copy(headers = (current.head.headers + header)))

  def withHeader(key: String, value: String): T = withHeader(HttpHeader(key, value))

  def withPath(path: String): T = transformHead(_.copy(path = path))

  def withMethod(method: HttpMethod): T = transformHead(_.copy(method = method))

  def withVersion(version: HttpVersion) : T = transformHead(_.copy(version = version))

  def withBody(body: HttpBody): T = next(current.copy(body = body))

}

trait HttpRequestBuilder[T] {

  def base: HttpRequest

  protected def build(f: HttpRequest): T

  def startMethod(method: HttpMethod, path: String) = build(base.withMethod(method).withPath(path))

  def get(path: String)    : T = startMethod(HttpMethod.Get, path)
  def post(path: String)   : T = startMethod(HttpMethod.Post, path)
  def put(path: String)    : T = startMethod(HttpMethod.Put, path)
  def delete(path: String) : T = startMethod(HttpMethod.Delete, path)
}

object HttpRequest extends HttpRequestBuilder[HttpRequest]{

  val base = HttpRequest(HttpMethod.Get, "/", HttpHeaders(), HttpBody.NoBody)

  protected def build(r: HttpRequest) = r

  def apply[T : HttpBodyEncoder](method: HttpMethod, url: String, headers: HttpHeaders, body: T): HttpRequest = {
    val head = BuiltHead(BuildFL(method, url, HttpVersion.`1.1`), headers)
    HttpRequest(head, HttpBody(body))
  }

  def apply[T : HttpBodyEncoder](method: HttpMethod, url: String, body: T): HttpRequest = {
    HttpRequest(method, url, HttpHeaders.Empty, body)
  }

}
