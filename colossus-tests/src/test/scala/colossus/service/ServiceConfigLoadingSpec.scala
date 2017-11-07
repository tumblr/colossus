package colossus.service

import colossus.core.ServerSettings
import colossus.parsing.DataSize._
import com.typesafe.config.ConfigException.WrongType
import org.scalatest.{MustMatchers, WordSpec}

import scala.concurrent.duration.Duration

class ServiceConfigLoadingSpec extends WordSpec with MustMatchers {

  "Service configuration loading" should {
    "load defaults" in {
      val config = ServerSettings.default
      config.logErrors mustBe true
      config.maxRequestSize mustBe 10.MB
      config.requestBufferSize mustBe 100
      config.requestMetrics mustBe true
      config.requestTimeout mustBe Duration.Inf
    }

    "load a config based on path with fallback to defaults" in {
      val config = ServerSettings.load("config-loading-spec")
      config.requestBufferSize mustBe 9876
      config.requestMetrics mustBe true
    }

    "throw a ServiceConfigException when something is wrong" in {
      intercept[WrongType] {
        ServerSettings.load("bad-config")
      }
    }

  }

}
