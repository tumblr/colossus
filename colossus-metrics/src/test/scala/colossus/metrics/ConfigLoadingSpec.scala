package colossus.metrics

import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

class ConfigLoadingSpec extends MetricIntegrationSpec {

  def config(userOverrides: String) = ConfigFactory.parseString(userOverrides).withFallback(ConfigFactory.defaultReference()).getConfig(MetricSystemConfig.ConfigRoot)

  def msconfig(userOverrides: String): MetricSystemConfig = MetricSystemConfig.load("test", config(userOverrides))

  def metricSystem(userOverrides: String): MetricSystem = MetricSystem(msconfig(userOverrides))

  "MetricSystem initialization" must {
    "load defaults from reference implementation" in {

      val ms = MetricSystemConfig.load("test")
      ms.name mustBe "test"
      ms.collectorConfig.intervals.toSet mustBe Set(1.second, 1.minute)
      ms.systemMetrics mustBe SystemMetricsConfig(true, "/test")

    }

    "fail to load if metric intervals contains an infinite value" in {
      val userOverrides =
        """
          |colossus.metrics.system{
          |  collection-intervals : ["Inf", "10 minutes"]
          |  namespace : "/mypath"
          |  collectors-defaults {
          |   rate {
          |    prune-empty : true
          |   }
          |  }
          |}
        """.stripMargin

      a[FiniteDurationExpectedException] must be thrownBy msconfig(userOverrides)
    }

    "fail to load if metric intervals contains a non duration string" in {
      val userOverrides =
        """
          |colossus.metrics.system{
          |  collection-intervals : ["foo", "10 minutes"]
          |  namespace : "/mypath"
          |  collectors-defaults {
          |   rate {
          |    prune-empty : true
          |   }
          |  }
          |}
        """.stripMargin

      a[NumberFormatException] must be thrownBy msconfig(userOverrides)
    }

    "load a dead system if the MetricSystem is disabled" in {
      val userOverrides = """
        |colossus.metrics.system{
        |  enabled : false
        |}
      """.stripMargin

      val ms = metricSystem(userOverrides)
      ms.config.name mustBe "DEAD"
    }
  }

  "Rate initialization" must {

    val userOverrides =
      """
        |colossus.metrics{
        |  pruned-rate {
        |    prune-empty : false
        |  }
        |  off-rate{
        |    enabled : false
        |  }
        |  system{
        |    collection-intervals : ["1 minute", "10 minutes"]
        |    collector-defaults {
        |     rate {
        |      prune-empty : true
        |     }
        |    }
        |  }
        |  mypath.config-rate{
        |    prune-empty : false
        |  }
        |}
      """.stripMargin
    implicit val ms = metricSystem(userOverrides)

    "load a rate using collector defaults" taggedAs(org.scalatest.Tag("test")) in {
      println(ms.config.collectorConfig.collectorDefaults.toString)
      val r = Rate(MetricAddress("my-rate"))
      r.pruneEmpty mustBe true
    }
    "load a rate using a defined configuration" in {
      val r = Rate(MetricAddress("my-pruned-rate"), "pruned-rate")
      r.pruneEmpty mustBe false
    }
    "load defaults if a defined configuration isn't found" in {
      val r = Rate(MetricAddress("my-crazy-rate"), "crazy-rate")
      r.pruneEmpty mustBe true
    }

    "load a NopRate when its definition is disabled" in {
      val r = Rate(MetricAddress("my-disabled-rate"), "off-rate")
      r mustBe a[NopRate]
    }

    "load a NopRate when the 'enabled' flag is set" in {
      val r = Rate(MetricAddress("/some-rate"), enabled = false)
      r mustBe a[NopRate]
    }
    "use a MetricAddress as the primary config source" in {
      val r = Rate(MetricAddress("mypath/config-rate"))
      r.pruneEmpty mustBe false
    }
  }

  "Histogram initialization" must {
    val userOverrides =
      """
        |colossus.metrics{
        |  small-hist {
        |    prune-empty : false
        |    percentiles : [.50, .75]
        |    buckets : {
        |     type: manual
        |     values: [0, 2, 4]
        |   }
        |  }
        |  off-hist{
        |    enabled : false
        |  }
        |  system{
        |    collection-intervals : ["1 minute", "10 minutes"]
        |    collector-defaults {
        |      histogram {
        |        prune-empty : true
        |        sample-rate : 1
        |      }
        |    }
        |  }
        |  mypath.config-hist{
        |    sample-rate : .33
        |    percentiles : [.25]
        |  }
        |}
      """.stripMargin
    implicit val ms = metricSystem(userOverrides)

    "load a Histogram using collector defaults" in {
      val r = Histogram(MetricAddress("my-hist"))
      r.pruneEmpty mustBe true
      r.sampleRate mustBe 1.0
      r.percentiles mustBe Seq(0.75, 0.9, 0.99, 0.999, 0.9999)
    }
    "load a Histogram using a defined configuration" in {
      val r = Histogram(MetricAddress("my-small-hist"), "small-hist")
      r.pruneEmpty mustBe false
      r.sampleRate mustBe 1.0
      r.percentiles mustBe Seq(0.5, 0.75)
      r.buckets mustBe BucketList(Vector(0, 2, 4))
    }
    "load defaults if a defined configuration isn't found" in {
      val r = Histogram(MetricAddress("my-crazy-hist"), "crazy-hist")
      r.pruneEmpty mustBe true
      r.sampleRate mustBe 1.0
      r.percentiles mustBe Seq(0.75, 0.9, 0.99, 0.999, 0.9999)
    }

    "load a NopHistogram when its definition is disabled" in {
      val r = Histogram(MetricAddress("my-disabled-hist"), "off-hist")
      r mustBe a[NopHistogram]
    }

    "load a NopHistogram when the 'enabled' flag is set" in {
      val r = Histogram(MetricAddress("some-hist"), enabled = false)
      r mustBe a[NopHistogram]
    }
    "use a MetricAddress as the primary config source" in {
      val r = Histogram(MetricAddress("mypath/config-hist"))
      r.sampleRate mustBe 0.33
      r.percentiles mustBe Seq(0.25)
    }
  }

  "Counter initialization" must {

    val userOverrides =
      """
        |colossus.metrics{
        |  off-counter{
        |    enabled : false
        |  }
        |  mypath.config-counter{
        |    enabled : false
        |  }
        |}
      """.stripMargin
    implicit val ms = metricSystem(userOverrides)

    "load a Counter using collector defaults" in {
      val r = Counter(MetricAddress("my-counter"))
      r mustBe a[DefaultCounter]
    }

    "load defaults if a defined configuration isn't found" in {
      val r = Counter(MetricAddress("my-crazy-counter"), "crazy-counter")
      r mustBe a[DefaultCounter]
    }

    "load a NopCounter when its definition is disabled" in {
      val r = Counter(MetricAddress("my-disabled-counter"), "off-counter")
      r mustBe a[NopCounter]
    }

    "load a NopCounter when the 'enabled' flag is set" in {
      val r = Counter(MetricAddress("some-counter"), enabled = false)
      r mustBe a[NopCounter]
    }
    "use a MetricAddress as the primary config source" in {
      val r = Counter(MetricAddress("mypath/config-counter"))
      r mustBe a[NopCounter]
    }
  }
}
