package colossus.service

import colossus.parsing.DataSize._
import org.scalatest.{WordSpec, MustMatchers}

import scala.concurrent.duration.Duration

class ServiceConfigLoadingSpec extends WordSpec with MustMatchers{

  "Service configuration loading" should {
    "load defaults" in {
      val config = ServiceConfig.Default
      config.logErrors mustBe true
      config.maxRequestSize mustBe 1.MB
      config.requestBufferSize mustBe 100
      config.requestMetrics mustBe true
      config.requestTimeout mustBe Duration.Inf
    }

    "load a config based on path with fallback to defaults" in {
      val config = ServiceConfig.load("config-loading-spec")
      config.requestBufferSize mustBe 9876
      config.requestMetrics mustBe true
    }

    "throw a ServiceConfigException when something is wrong" in {
      intercept[ServiceConfigException] {
        ServiceConfig.load("bad-config")
      }
    }


  }

}
