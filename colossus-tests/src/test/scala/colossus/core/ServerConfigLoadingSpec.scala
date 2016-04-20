package colossus.core

import colossus.EchoHandler
import colossus.metrics.{DefaultRate, NopCounter, MetricAddress}
import colossus.testkit.ColossusSpec
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

class ServerConfigLoadingSpec  extends ColossusSpec {

  "Server configuration loading" should {
    "load defaults" in {
      withIOSystem{ implicit io =>
        val s = Server.basic()(context => new EchoHandler(context))
        waitForServer(s)
        s.name mustBe MetricAddress.Root
        val settings = s.config.settings
        settings.bindingAttemptDuration mustBe PollingDuration(200.milliseconds, None)
        settings.delegatorCreationDuration mustBe PollingDuration(500.milliseconds, None)
        settings.highWatermarkPercentage mustBe 0.85
        settings.lowWatermarkPercentage mustBe 0.75
        settings.maxConnections mustBe 1000
        settings.maxIdleTime mustBe Duration.Inf
        settings.port mustBe 9876
        settings.shutdownTimeout mustBe 100.milliseconds
        settings.tcpBacklogSize mustBe None
      }
    }

    "load user overrides" in {
      val userOverrides =
        """{
          | my-server{
          |   name = "mine"
          |    port : 9888
          |    max-connections : 1000
          |    max-idle-time : "1 second"
          |    tcp-backlog-size : 100
          |    shutdown-timeout : "2 seconds"
          | }
          |}
        """.stripMargin
      val c = ConfigFactory.parseString(userOverrides).withFallback(ConfigFactory.defaultReference())
      withIOSystem{ implicit io =>
        val s = Server.basic("my-server", c)(context => new EchoHandler(context))
        waitForServer(s)
        s.name mustBe MetricAddress("mine")
        val settings = s.config.settings
        settings.bindingAttemptDuration mustBe PollingDuration(200.milliseconds, None)
        settings.delegatorCreationDuration mustBe PollingDuration(500.milliseconds, None)
        settings.highWatermarkPercentage mustBe 0.85
        settings.lowWatermarkPercentage mustBe 0.75
        settings.maxConnections mustBe 1000
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
        settings.bindingAttemptDuration mustBe PollingDuration(200.milliseconds, None)
        settings.delegatorCreationDuration mustBe PollingDuration(500.milliseconds, None)
        settings.highWatermarkPercentage mustBe 0.85
        settings.lowWatermarkPercentage mustBe 0.75
        settings.maxConnections mustBe 1000
        settings.maxIdleTime mustBe Duration.Inf
        settings.port mustBe 8989
        settings.shutdownTimeout mustBe 100.milliseconds
        settings.tcpBacklogSize mustBe None
      }
    }
    "not explode if there is no 'metrics' configuration" in {
      withIOSystem{ implicit io =>
        //this loads the entire config, but the this constructor wants a Config which is pointing at a Server configuration
        val s = Server.basic("my-server", ServerSettings(8989), ConfigFactory.load())(context => new EchoHandler(context))
        waitForServer(s)
        //no explosions, means we are good
      }
    }
    "metrics should be configured correctly" in {
      val userOverrides =
        """
          | my-server{
          |   name : "mine"
          |    port : 9888
          |    metrics{
          |     connections {
          |        enabled : false
          |      }
          |      refused-connections {
          |        prune-empty : true
          |      }
          |      connects {
          |        prune-empty : true
          |      }
          |      closed {
          |        prune-empty : true
          |      }
          |    }
          | }
          |
        """.stripMargin
      val c = ConfigFactory.parseString(userOverrides).withFallback(ConfigFactory.defaultReference())
      withIOSystem{ implicit io =>
        val s = Server.basic("my-server", c)(context => new EchoHandler(context))
        waitForServer(s)
        val collectors = io.metrics.collection.collectors
        val sRoot = s.namespace.namespace
        collectors.get(sRoot / "connections") mustBe a[NopCounter]
        val rates = Seq("refused_connections", "connects", "closed")
        rates.foreach{ x =>
          val r = collectors.get(sRoot / x).asInstanceOf[DefaultRate]
          r.pruneEmpty mustBe true
        }
        //didn't override
        collectors.get(sRoot / "highwaters").asInstanceOf[DefaultRate].pruneEmpty mustBe false
      }
    }
  }
}
