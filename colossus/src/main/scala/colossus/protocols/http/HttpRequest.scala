package colossus
package protocols.http

import service._

import akka.util.ByteString

case class HttpRequest(head: HttpHead, entity: Option[ByteString]) {
  import head._
  import HttpCodes._
  import HttpParse._
  import Response._

  def complete(response: HttpResponse): Completion[HttpResponse] = {
    val tags = Map("status_code" -> response.code.code.toString)
    
    if (head.version == HttpVersion.`1.0` /*|| head.headers.get(HttpHeaders.Connection).exists{_ == "close"} */) {
      new Completion(response, onwrite = OnWriteAction.Disconnect, tags = tags)
    } else {
      Completion(response, tags = tags)
    }
  }

  def respond(code: HttpCode, data: String, headers: List[(String, String)] = Nil) = complete(HttpResponse(version, code, ByteString(data), headers))

  def ok(data: String, headers: List[(String, String)] = Nil)              = respond(OK, data, headers)
  def notFound(data: String = "", headers: List[(String, String)] = Nil)   = respond(NOT_FOUND, data, headers)
  def error(message: String, headers: List[(String, String)] = Nil)        = respond(INTERNAL_SERVER_ERROR, message, headers)
  def badRequest(message: String, headers: List[(String, String)] = Nil)   = respond(BAD_REQUEST, message, headers)
  def unauthorized(message: String, headers: List[(String, String)] = Nil) = respond(UNAUTHORIZED, message, headers)


  //TODO optimize
  def bytes : ByteString = {
    head.bytes ++ entity.getOrElse(ByteString())
  }

  def withHeader(key: String, value: String) = copy(head = head.withHeader(key, value))
}

object HttpRequest {
  def apply(method: HttpMethod, url: String, body: Option[String]): HttpRequest = {
    val bodybytes = body.map{ByteString(_)}
    val head = HttpHead(method, url, HttpVersion.`1.1`, List((HttpHeaders.ContentLength -> bodybytes.map{_.size.toString}.getOrElse("0"))))
    //val head = HttpHead(method, url, HttpVersion.`1.1`, Map(HttpHeaders.ContentLength -> List(bodybytes.map{_.size.toString}.getOrElse("0"))))
    HttpRequest(head, bodybytes)
  }
}
