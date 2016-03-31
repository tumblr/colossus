package colossus

import colossus.metrics.MetricAddress
import colossus.testkit.ColossusSpec
import com.typesafe.config.ConfigFactory

class IOSystemConfigSpec extends ColossusSpec{

  "IOSystem creation" must {
    "load defaults from reference implementation" in {
      val io = IOSystem()
      io.numWorkers mustBe Runtime.getRuntime.availableProcessors()
      io.name mustBe "/"
      io.metrics.namespace mustBe MetricAddress.Root
      shutdownIOSystem(io)
    }

    "apply user overridden configuration" in {
      val userConfig = """
                        |my-io-system{
                        |  name : "/my/path"
                        |  metrics : {
                        |    namespace : "/m"
                        |  }
                        |}
                      """.stripMargin

      //to imitate an already loaded configuration
      val c = ConfigFactory.parseString(userConfig).withFallback(ConfigFactory.defaultReference())
      val io = IOSystem("my-io-system", c)
      io.numWorkers mustBe Runtime.getRuntime.availableProcessors()
      io.metrics.namespace mustBe MetricAddress("/m")
      io.name mustBe "/my/path"
      io.namespace mustBe MetricAddress("/m/my/path")
      shutdownIOSystem(io)
    }
  }
}
