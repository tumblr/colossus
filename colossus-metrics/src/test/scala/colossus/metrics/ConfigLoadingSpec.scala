package colossus.metrics

import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

class ConfigLoadingSpec extends MetricIntegrationSpec {

  val PrefixRoot = "colossus.metrics."

  val expectedPaths = Seq("collect-system-metrics", "metric-intervals", "metric-address", "collector-defaults.rate.prune-empty",
                          "collector-defaults.histogram.prune-empty", "collector-defaults.histogram.sample-rate",
                          "collector-defaults.histogram-percentiles")

  "MetricSystem initialization" must {
    "load defaults from reference implementation" in {

      val ms = MetricSystem()
      ms.namespace mustBe MetricAddress.Root
      ms.metricIntervals.keys mustBe Set(1.second, 1.minute)
      ms.collectionSystemMetrics mustBe true

      expectedPaths.foreach{ x =>
        ms.config.hasPath(s"$PrefixRoot$x")
      }
    }

    "apply user overridden configuration" in {
      val userOverrides =
        """
          |my-metrics{
          |  metric-intervals : ["1 minute", "10 minutes"]
          |  metric-address : "/mypath"
          |  collectors-defaults {
          |   rate {
          |    prune-empty : true
          |   }
          |  }
          |}
        """.stripMargin

      //to imitate an already loaded configuration
      val c = ConfigFactory.parseString(userOverrides).withFallback(ConfigFactory.defaultReference())

      val ms = MetricSystem("my-metrics", c)

      ms.namespace mustBe MetricAddress("/mypath")
      ms.metricIntervals.keys mustBe Set(1.minute, 10.minutes)
      ms.collectionSystemMetrics mustBe true


      expectedPaths.foreach{ x =>
        ms.config.hasPath(s"$PrefixRoot$x")
      }
    }

    "fail to load if metric intervals contains an infinite value" in {
      val userOverrides =
        """
          |my-metrics{
          |  metric-intervals : ["Inf", "10 minutes"]
          |  metric-address : "/mypath"
          |  collectors-defaults {
          |   rate {
          |    prune-empty : true
          |   }
          |  }
          |}
        """.stripMargin

      val c = ConfigFactory.parseString(userOverrides).withFallback(ConfigFactory.defaultReference())
      a[FiniteDurationExpectedException] must be thrownBy MetricSystem("my-metrics", c)

    }

    "fail to load if metric intervals contains a non duration string" in {
      val userOverrides =
        """
          |my-metrics{
          |  metric-intervals : ["foo", "10 minutes"]
          |  metric-address : "/mypath"
          |  collectors-defaults {
          |   rate {
          |    prune-empty : true
          |   }
          |  }
          |}
        """.stripMargin

      val c = ConfigFactory.parseString(userOverrides).withFallback(ConfigFactory.defaultReference())
      a[NumberFormatException] must be thrownBy MetricSystem("my-metrics", c)

    }

  }

  "Rate initialization" must {
    "load configuration in the right order" in {
      val userOverrides =
        """
          |my-metrics{
          |  /myrate {
          |    prune-empty : false
          |  }
          |  metric-intervals : ["1 minute", "10 minutes"]
          |  metric-address : "/mypath"
          |  collectors-defaults {
          |   rate {
          |    prune-empty : true
          |   }
          |  }
          |}
        """.stripMargin

      //to imitate an already loaded configuration
      val c = ConfigFactory.parseString(userOverrides).withFallback(ConfigFactory.defaultReference())
      val ms = MetricSystem("my-metrics", c)
      implicit val m = ms.base

      val r = Rate(MetricAddress("/myrate"))
      r.pruneEmpty mustBe false

      val r2 = Rate(MetricAddress("/foo"))
      r2.pruneEmpty mustBe true

    }

    "fall back on default collector values" in {
      val userOverridesNoDefaults =
        """
          |my-metrics{
          |  "/myrate" {
          |    prune-empty : false
          |  }
          |  metric-intervals : ["1 minute", "10 minutes"]
          |}
        """.stripMargin

      //to imitate an already loaded configuration
      val c2 = ConfigFactory.parseString(userOverridesNoDefaults).withFallback(ConfigFactory.defaultReference())
      val ms2 = MetricSystem("my-metrics", c2)

      implicit val m2 = ms2.base

      val r3 = Rate(MetricAddress("/foo"))
      r3.pruneEmpty mustBe false
    }

    "load configuration in the right order, when supplied with a configuration path" in {
      val userOverrides =
        """
          |my-metrics{
          |  metric-intervals : ["1 minute", "10 minutes"]
          |  metric-address : "/mypath"
          |  collectors-defaults {
          |   rate {
          |    prune-empty : true
          |   }
          |  }
          |}
          |my-app{
          | "/myrate" {
          |   prune-empty : false
          | }
          |}
        """.stripMargin

      //to imitate an already loaded configuration
      val c = ConfigFactory.parseString(userOverrides).withFallback(ConfigFactory.defaultReference())
      val ms = MetricSystem("my-metrics", c)

      implicit val m = ms.base

      val r = Rate(MetricAddress("/myrate"), "my-app")
      r.pruneEmpty mustBe false

      val r2 = Rate(MetricAddress("/foo"), "my-app")
      r2.pruneEmpty mustBe true
    }
  }

  "Histogram initialization" must {
    "load configuration in the right order" in {
      val userOverrides =
        """
          |my-metrics{
          |  /myhist {
          |    prune-empty : false
          |    percentiles : [.50, .75]
          |  }
          |  metric-intervals : ["1 minute", "10 minutes"]
          |  metric-address : "/mypath"
          |  collectors-defaults {
          |   histogram {
          |    prune-empty : true
          |    sample-rate : 1
          |   }
          |  }
          |}
        """.stripMargin

      //to imitate an already loaded configuration
      val c = ConfigFactory.parseString(userOverrides).withFallback(ConfigFactory.defaultReference())
      val ms = MetricSystem("my-metrics", c)
      implicit val m = ms.base

      val h: Histogram = Histogram(MetricAddress("/myhist"))
      h.percentiles mustBe Seq(.50, .75)
      h.pruneEmpty mustBe false
      h.sampleRate mustBe 1

      val h2 = Histogram(MetricAddress("/foo"))
      h2.pruneEmpty mustBe true
      h2.sampleRate mustBe 1
      h2.percentiles mustBe Seq(0.75, 0.9, 0.99, 0.999, 0.9999)
    }

    "load configuration in the right order, when supplied with a configuration path" in {
      val userOverrides =
        """
          |my-metrics{
          |  metric-intervals : ["1 minute", "10 minutes"]
          |  metric-address : "/mypath"
          |  collectors-defaults {
          |   histogram {
          |    prune-empty : true
          |    sample-rate : 1
          |   }
          |  }
          |}
          |my-app{
          | "/myhist" {
          |   prune-empty : false
          |   sample-rate : .5
          | }
          |}
        """.stripMargin

      //to imitate an already loaded configuration
      val c = ConfigFactory.parseString(userOverrides).withFallback(ConfigFactory.defaultReference())
      val ms = MetricSystem("my-metrics", c)

      implicit val m = ms.base

      val h = Histogram(MetricAddress("/myhist"), "my-app")
      h.pruneEmpty mustBe false
      h.sampleRate mustBe .5
      h.percentiles mustBe Seq(0.75, 0.9, 0.99, 0.999, 0.9999)


      val h2 = Histogram(MetricAddress("/foo"), "my-app")
      h2.pruneEmpty mustBe true
      h2.sampleRate mustBe 1
      h2.percentiles mustBe Seq(0.75, 0.9, 0.99, 0.999, 0.9999)
    }
  }




}