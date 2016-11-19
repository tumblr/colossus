package colossus.core

import colossus.EchoHandler
import colossus.metrics.MetricAddress
import colossus.testkit.ColossusSpec
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

class ServerConfigLoadingSpec  extends ColossusSpec {

  val refBindingRetry =  BackoffPolicy(100.milliseconds, BackoffMultiplier.Exponential(1.second), immediateFirstAttempt = false)
  val refDelegatorCreationPolicy = WaitPolicy(500.milliseconds, BackoffPolicy(100.milliseconds, BackoffMultiplier.Constant, immediateFirstAttempt = false))

  "Server configuration loading" should {
    "load defaults" in {
      withIOSystem{ implicit io =>
        val s = Server.basic("my-server")(context => new EchoHandler(context))
        waitForServer(s)
        s.name mustBe MetricAddress("my-server")
        val settings = s.config.settings
        settings.bindingRetry mustBe refBindingRetry
        settings.delegatorCreationPolicy mustBe refDelegatorCreationPolicy
        settings.highWatermarkPercentage mustBe 0.85
        settings.lowWatermarkPercentage mustBe 0.75
        settings.maxConnections mustBe 1000
        settings.slowStart.initial mustBe 20
        settings.maxIdleTime mustBe Duration.Inf
        settings.port mustBe 9876
        settings.shutdownTimeout mustBe 100.milliseconds
        settings.tcpBacklogSize mustBe None
      }
    }

    "load user overrides" in {
      val userOverrides =
        """colossus.server.my-server{
          |    port : 9888
          |    max-connections : 1000
          |    max-idle-time : "1 second"
          |    shutdown-timeout : "2 seconds"
          |}
          |colossus.server {
          |   shutdown-timeout : "3 seconds"
          |    tcp-backlog-size : 100
          |}
        """.stripMargin
      val c = ConfigFactory.parseString(userOverrides).withFallback(ConfigFactory.defaultReference())
      withIOSystem{ implicit io =>
        val s = Server.basic("my-server", c)(context => new EchoHandler(context))
        waitForServer(s)
        s.name mustBe MetricAddress("my-server")
        val settings = s.config.settings
        settings.bindingRetry mustBe refBindingRetry
        settings.delegatorCreationPolicy mustBe refDelegatorCreationPolicy
        settings.highWatermarkPercentage mustBe 0.85
        settings.lowWatermarkPercentage mustBe 0.75
        settings.maxConnections mustBe 1000
        settings.slowStart.initial mustBe 20
        settings.maxIdleTime mustBe 1.second
        settings.port mustBe 9888
        settings.shutdownTimeout mustBe 2.seconds
        settings.tcpBacklogSize mustBe Some(100)
      }
    }

    "quick config" in {
      withIOSystem{ implicit io =>
        val s = Server.basic("quick-server", 8989)(context => new EchoHandler(context))
        waitForServer(s)
        s.name mustBe MetricAddress("quick-server")
        val settings = s.config.settings
        settings.bindingRetry mustBe refBindingRetry
        settings.delegatorCreationPolicy mustBe refDelegatorCreationPolicy
        settings.highWatermarkPercentage mustBe 0.85
        settings.lowWatermarkPercentage mustBe 0.75
        settings.maxConnections mustBe 1000
        settings.slowStart.initial mustBe 20
        settings.maxIdleTime mustBe Duration.Inf
        settings.port mustBe 8989
        settings.shutdownTimeout mustBe 100.milliseconds
        settings.tcpBacklogSize mustBe None
      }
    }
  }
}
