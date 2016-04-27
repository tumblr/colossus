package colossus
package protocols.http

import HttpMethod._

import testkit._

class HttpRequestSpec extends ColossusSpec {


  "HttpRequestBuilder" must {
    "build a request" in {

      HttpRequest.get("/foo") must equal(HttpRequest.base.withMethod(Get).withPath("/foo"))
      HttpRequest.post("/foo") must equal(HttpRequest.base.withMethod(Post).withPath("/foo"))
      HttpRequest.put("/foo") must equal(HttpRequest.base.withMethod(Put).withPath("/foo"))
      HttpRequest.delete("/foo") must equal(HttpRequest.base.withMethod(Delete).withPath("/foo"))
    }
  }


}
