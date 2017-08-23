package colossus.testkit

import colossus.service._
import colossus.protocols.http._
import java.net.InetSocketAddress
import scala.concurrent.duration._

class MockClientSpec extends ColossusSpec {

  implicit val ex = FakeIOSystem.testExecutor

  "MockClient" must {
    "work" in {
      implicit val worker = FakeIOSystem.fakeWorker.worker
      val c: HttpClient[Callback] =
        Http.client(MockSender[Http, Callback]((request: HttpRequest) => Callback.successful(request.ok("hi"))))

      CallbackAwait.result(c.send(HttpRequest.get("foo")), 1.second).body.asDataBlock.utf8String mustBe "hi"
    }
  }

  "MockClientFactory" must {
    "work with LoadBalancingClient" in {
      implicit val worker = FakeIOSystem.fakeWorker.worker
      val l: LoadBalancingClient[Http] = new LoadBalancingClient(
        List(new InetSocketAddress("1.1.1.1", 34)),
        ClientConfig(address = new InetSocketAddress("0.0.0.0", 1), name = "/foo", requestTimeout = 1.second),
        MockClientFactory.client[Http]((x: HttpRequest) => Callback.successful(x.ok("test"))),
        4
      )

      CallbackAwait.result(l.send(HttpRequest.get("foo")), 1.second).body.asDataBlock.utf8String mustBe "test"
    }
  }

}
