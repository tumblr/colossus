package colossus.service

import java.net.InetSocketAddress

import colossus.metrics.MetricAddress
import com.typesafe.config.ConfigFactory
import org.scalatest.{MustMatchers, WordSpec}
import scala.concurrent.duration._

class ClientConfigLoadingSpec extends WordSpec with MustMatchers{

  "ClientConfig loading" must {

    "load a user defined client using fallbacks" in {
      val userOverrides =
        """
          |colossus.client.my-client {
          |  address : "127.0.0.1:6379"
          |  name : "redis"
          |  pending-buffer-size : 500
          |}
        """.stripMargin

      val c = ConfigFactory.parseString(userOverrides).withFallback(ConfigFactory.defaultReference())
      val clientConfig = ClientConfig.load("my-client", c)

      val expected = ClientConfig(new InetSocketAddress("127.0.0.1", 6379), requestTimeout = 1.second, name = MetricAddress("redis"), pendingBufferSize = 500)

      clientConfig mustBe expected
    }
  }
}
