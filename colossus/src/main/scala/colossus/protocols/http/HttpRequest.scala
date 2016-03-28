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


case class HttpRequestHead(firstLine: FirstLine, headers: HttpHeaders) extends Encoder {

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
    buffer write HttpParse.NEWLINE_ARRAY
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
    HttpRequestHead(BuildFL(method, url, version), headers)

  }
}

case class HttpRequest(head: HttpRequestHead, body: HttpBody) extends Encoder {
  import head._
  import HttpCodes._

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
    //TODO : write content-length
    body encode buffer
  }

  def withHeader(key: String, value: String) = copy(head = head.withHeader(key, value))
}

object HttpRequest {

  def apply[T : HttpBodyEncoder](method: HttpMethod, url: String, headers: HttpHeaders, body: T): HttpRequest = {
    val head = HttpRequestHead(BuildFL(method, url, HttpVersion.`1.1`), headers)
    HttpRequest(head, HttpBody(body))
  }

  def apply[T : HttpBodyEncoder](method: HttpMethod, url: String, body: T): HttpRequest = {
    HttpRequest(method, url, HttpHeaders.Empty, body)
  }

  def get(url: String) = HttpRequest(HttpMethod.Get, url, HttpBody.NoBody)
}
