package colossus.testkit

import colossus.protocols.http.{Http, HttpCode, HttpRequest, HttpResponse}

import scala.concurrent.Await

abstract class HttpServiceSpec extends ServiceSpec[Http] {

  def expectCode(request: HttpRequest, expectedResponseCode: HttpCode): Unit = {
    withClient { client =>
      val resp = Await.result(client.send(request), requestTimeout)
      assertCode(request, resp, expectedResponseCode)
    }
  }

  def expectCodeAndBody(request: HttpRequest, expectedResponseCode: HttpCode, expectedBody: String): Unit = {
    withClient { client =>
      val resp = Await.result(client.send(request), requestTimeout)
      assertCode(request, resp, expectedResponseCode)
      assertBody(request, resp, expectedBody)
    }
  }

  def expectCodeAndBodyPredicate(request: HttpRequest, expectedResponseCode: HttpCode)(
      bodyPredicate: String => Boolean): Unit = {
    withClient { client =>
      val response = Await.result(client.send(request), requestTimeout)
      assertCode(request, response, expectedResponseCode)
      val body = response.body.bytes.utf8String
      assert(bodyPredicate(body))
    }
  }

  def assertCode(request: HttpRequest, response: HttpResponse, expectedCode: HttpCode): Unit = {
    val msg = s"${request.head.method} ${request.head.url}: Code ${response.head.code} did not match $expectedCode. " +
      s"Body: ${response.body.bytes.utf8String}"
    assert(response.head.code == expectedCode, msg)
  }

  def assertBody(request: HttpRequest, response: HttpResponse, expectedBody: String): Unit = {
    val body = response.body.bytes.utf8String
    assert(body == expectedBody, s"$body did not equal $expectedBody")
  }

}
