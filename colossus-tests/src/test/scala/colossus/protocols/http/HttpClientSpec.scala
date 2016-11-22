package colossus
package protocols.http

import java.net.InetSocketAddress

import colossus.metrics.MetricAddress
import colossus.service.{ClientConfig, Sender}
import colossus.testkit.ColossusSpec
import org.scalamock.scalatest.MockFactory

import scala.concurrent.Future
import scala.concurrent.duration.Duration

class HttpClientSpec extends ColossusSpec with MockFactory {

  "Http Client" must {

    val clientConfig = ClientConfig(
      address = new InetSocketAddress("localhost", 80),
      requestTimeout = Duration.Inf,
      name = MetricAddress.Root / "testMetric"
    )

    "include the Host header into a request if it's missing" in {
      withIOSystem { implicit io =>
        val request = HttpRequest.get("/")

        val sender = mock[Sender[Http, Future]]

        val expectedRequest = request.withHeader("host", "localhost")
        (sender.send _).expects(expectedRequest)

        val httpClient = Http.futureClient(sender, clientConfig)
        httpClient.send(request)
      }
    }

    "not modify the existing Host header" in {
      withIOSystem { implicit io =>
        val request = HttpRequest.get("/").withHeader("host", "some.host")

        val sender = mock[Sender[Http, Future]]
        (sender.send _).expects(request)

        val httpClient = Http.futureClient(sender, clientConfig)
        httpClient.send(request)
      }
    }
  }
}
