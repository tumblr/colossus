package colossus
package testkit

import scala.concurrent.Await
import akka.util.ByteString

import core._
import service._

import protocols.http._

import net.liftweb.json._

abstract class HttpServiceSpec extends ServiceSpec[Http] {

  implicit val formats = DefaultFormats

  def expectCode(request: HttpRequest, expectedResponseCode: HttpCode) {
    withClient{client =>
      val resp = Await.result(client.send(request), requestTimeout)
      val msg = request.head.method + " " + request.head.url + ": Code " + resp.code + " did not match " + expectedResponseCode + ". Body: " + resp.data.utf8String
      assert(resp.code == expectedResponseCode, msg)
    }
  }

  def get(url: String) = HttpRequest(HttpHead(version = HttpVersion.`1.1`, method = HttpMethod.Get, url = url, headers = Nil), None)

  def testGetCode(url : String, expectedCode : HttpCode) {
    val req = get(url)
    expectCode(req, expectedCode)
  }

  def testGet(url: String, expectedBody: String) {
    val req = get(url)
    val expected = HttpResponse(HttpVersion.`1.1`, HttpCodes.OK, ByteString(expectedBody), List(("content-length" -> expectedBody.length.toString)))

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
      val parsed = parse(resp.data.utf8String).extract[T]
      parsed must equal(expectedJson)
    }
  }

}

