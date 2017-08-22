/*
import colossus.protocols.http.HttpRequest
import colossus.testkit.CallbackAwait
import colossus.testkit.MockConnection
import org.scalatest.{MustMatchers, WordSpec}

import scala.concurrent.duration._

// #example
class TestkitExampleSpec extends WordSpec with MustMatchers {
  "request handler" must {
    "generate a response" in {
      val connection = MockConnection.server(new MyHandler(_))
      val response = connection.typedHandler.handle(HttpRequest.get("/foo"))
      CallbackAwait.result(response, 1.second).body.bytes.utf8String mustBe "hello"
    }
  }
}*/
// #example
