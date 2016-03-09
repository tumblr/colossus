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
      response.body.bytes.utf8String
    assert(response.head.code == expectedCode, msg)
  }

    
  def assertBody(request : HttpRequest, response : HttpResponse, expectedBody : String){
    val body = response.body.bytes.utf8String
    assert(body == expectedBody, s"$body did not equal $expectedBody")
  }

  def expectJson[T : Manifest](request : HttpRequest, expectedJson : T) {
    withClient{ client =>
      val resp = Await.result(client.send(request), requestTimeout)
      val parsed = parse(resp.body.bytes.utf8String).extract[T]
      parsed must equal(expectedJson)
    }
  }

}

