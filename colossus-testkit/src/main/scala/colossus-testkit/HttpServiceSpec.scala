package colossus
package testkit

import scala.concurrent.Await
import akka.util.ByteString

import protocols.http._

import net.liftweb.json._

abstract class HttpServiceSpec extends ServiceSpec[Http] {

  implicit val formats = DefaultFormats

  def expectCode(request: HttpRequest, expectedResponseCode: HttpCode) {
    withClient{client =>
      val resp = Await.result(client.send(request), requestTimeout)
      assertCode(request, resp, expectedResponseCode)
    }
  }

  def expectCodeAndBody(request: HttpRequest, expectedResponseCode: HttpCode, body : String) {
    withClient{client =>
      val resp = Await.result(client.send(request), requestTimeout)
      assertCode(request, resp, expectedResponseCode)
      assertBody(request, resp, body)
    }
  }

  def assertCode(request : HttpRequest, response : HttpResponse, expectedCode : HttpCode) {
    val msg = request.head.method +
      " " + request.head.url + ": Code " +
      response.head.code + " did not match " +
      expectedCode + ". Body: "  +
      response.body.map{_.utf8String}.getOrElse("(none)")
    assert(response.head.code == expectedCode, msg)
  }

  def assertBody(request : HttpRequest, response : HttpResponse, expectedBody : String){
    response.body match {
      case None => throw new Exception(s"No body detected, expected $expectedBody")
      case Some(x) => assert(x.utf8String == expectedBody, s"${x.utf8String} did not equal $expectedBody")
    }
  }

  def get(url: String) = HttpRequest(HttpHead(version = HttpVersion.`1.1`, method = HttpMethod.Get, url = url, headers = Nil), None)

  def testGetCode(url : String, expectedCode : HttpCode) {
    val req = get(url)
    expectCode(req, expectedCode)
  }

  def testGet(url: String, expectedBody: String) {
    val req = get(url)
    val expected = HttpResponse(
      HttpResponseHead(HttpVersion.`1.1`, HttpCodes.OK, Vector(HttpResponseHeader("content-length" ,expectedBody.length.toString))), 
      Some(ByteString(expectedBody))
    )

    expectResponse(req, expected)
  }

  def withGet(url: String, tester: HttpResponse => Any) {
    val request = get(url)
    withClient{ client =>
      val resp = Await.result(client.send(request), requestTimeout)
      tester(resp)
    }
  }

  def testPost(url: String, data: String, expectedCode: HttpCode) {
    val req = HttpRequest(HttpHead(version = HttpVersion.`1.1`, method = HttpMethod.Post, url = url, headers = List(("content-length" ->data.length.toString))), Some(ByteString(data)))
    expectCode(req, expectedCode)
  }

  def testPost(url: String, data: String, expectedCode: HttpCode, expectedBody : String) {
    val req = HttpRequest(HttpHead(version = HttpVersion.`1.1`, method = HttpMethod.Post, url = url, headers = List(("content-length" ->data.length.toString))), Some(ByteString(data)))
    expectCodeAndBody(req, expectedCode, expectedBody)
  }

  def testDelete(url: String, expectedCode: HttpCode) {
    val req = HttpRequest(HttpHead(version = HttpVersion.`1.1`, method = HttpMethod.Delete, url = url, headers = List()), None)
    expectCode(req, expectedCode)
  }
    
    
  def testGetJson[T : Manifest](url : String, expectedResponse : T) {
    val req = get(url)
    expectJson(req, expectedResponse)
  }

  def expectJson[T : Manifest](request : HttpRequest, expectedJson : T) {
    withClient{ client =>
      val resp = Await.result(client.send(request), requestTimeout)
      val parsed = parse(resp.body.get.utf8String).extract[T]
      parsed must equal(expectedJson)
    }
  }

}

