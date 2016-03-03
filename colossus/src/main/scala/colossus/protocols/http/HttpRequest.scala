package colossus
package protocols.http

import akka.util.ByteString

case class HttpRequestHead(method: HttpMethod, url: String, version: HttpVersion, headers: HttpHeaders) {

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

  lazy val cookies: Seq[Cookie] = headers.multiHeader(HttpHeaders.CookieHeader).flatMap{Cookie.parseHeader}

  //we should only encode if the string is decoded.
  //To check for that, if we decode an already decoded URL, it should not change (this may not be necessary)
  //the alternative is one big fat honkin ugly regex and knowledge of what characters are allowed where(gross)
  //TODO: this doesn't work, "/test" becomes %2Ftest
  private def getEncodedURL : String = url
  /*{
    if(URLDecoder.decode(url,"UTF-8") == url) {
      URLEncoder.encode(url, "UTF-8")
    }else {
      url
    }
  }*/


  //TODO: optimize
  def bytes : ByteString = {
    val reqString = ByteString(s"${method.name} $getEncodedURL HTTP/$version\r\n")
    if (headers.size > 0) {
      val buf = new core.DynamicOutBuffer(200, false)
      headers.encode(buf)
      val encodedHeaders = ByteString(buf.data.data)
      reqString ++ encodedHeaders ++ ByteString("\r\n\r\n")
    } else {
      reqString ++ ByteString("\r\n")
    }
  }

  def persistConnection: Boolean =
    (version, headers.connection) match {
      case (HttpVersion.`1.1`, Some(Connection.Close)) => false
      case (HttpVersion.`1.1`, _) => true
      case (HttpVersion.`1.0`, Some(Connection.KeepAlive)) => true
      case (HttpVersion.`1.0`, _) => false
    }
}

case class HttpRequest(head: HttpRequestHead, entity: Option[ByteString]) {
  import head._
  import HttpCodes._

  def respond(code: HttpCode, data: String, headers: List[(String, String)] = Nil) = {
    import HttpHeader.Conversions._
    HttpResponse(HttpResponseHead(version, code, headers), Some(ByteString(data)))
  }

  def ok(data: String, headers: List[(String, String)] = Nil)              = respond(OK, data, headers)
  def notFound(data: String = "", headers: List[(String, String)] = Nil)   = respond(NOT_FOUND, data, headers)
  def error(message: String, headers: List[(String, String)] = Nil)        = respond(INTERNAL_SERVER_ERROR, message, headers)
  def badRequest(message: String, headers: List[(String, String)] = Nil)   = respond(BAD_REQUEST, message, headers)
  def unauthorized(message: String, headers: List[(String, String)] = Nil) = respond(UNAUTHORIZED, message, headers)
  def forbidden(message: String, headers: List[(String, String)] = Nil)    = respond(FORBIDDEN, message, headers)

  //TODO optimize
  def bytes : ByteString = {
    head.bytes ++ entity.getOrElse(ByteString())
  }

  def withHeader(key: String, value: String) = copy(head = head.withHeader(key, value))
}

object HttpRequest {
  import HttpHeader.Conversions._
  def apply(method: HttpMethod, url: String, body: Option[String]): HttpRequest = {
    val bodybytes = body.map{ByteString(_)}
    val head = HttpRequestHead(method, url, HttpVersion.`1.1`, List((HttpHeaders.ContentLength -> bodybytes.map{_.size.toString}.getOrElse("0"))))
    //val head = HttpHead(method, url, HttpVersion.`1.1`, Map(HttpHeaders.ContentLength -> List(bodybytes.map{_.size.toString}.getOrElse("0"))))
    HttpRequest(head, bodybytes)
  }


  def get(url: String) = HttpRequest(HttpMethod.Get, url, None)
}
