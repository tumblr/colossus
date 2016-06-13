package colossus

import colossus.metrics.MetricAddress
import colossus.testkit.ColossusSpec
import com.typesafe.config.ConfigFactory

class IOSystemConfigSpec extends ColossusSpec{

  "IOSystem creation" must {

    "load defaults from reference implementation" in {
      val io = IOSystem()
      io.numWorkers mustBe Runtime.getRuntime.availableProcessors()
      io.name mustBe "iosystem"
      io.metrics.namespace mustBe MetricAddress.Root
      shutdownIOSystem(io)
    }

    "apply user overridden configuration" in {
      val userConfig = """
                        |colossus.iosystem{
                        |  num-workers : 2
                        |}
                      """.stripMargin

      //to imitate an already loaded configuration
      val c = ConfigFactory.parseString(userConfig).withFallback(ConfigFactory.defaultReference())
      val io = IOSystem("my-io-system", c.getConfig(IOSystem.ConfigRoot))
      io.numWorkers mustBe 2
      io.name mustBe "my-io-system"
      shutdownIOSystem(io)
    }
  }
}
