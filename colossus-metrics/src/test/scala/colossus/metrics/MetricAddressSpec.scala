package colossus.metrics

import org.scalatest.{MustMatchers, WordSpec}

class MetricAddressSpec extends WordSpec with MustMatchers{

  "MetricAddress" must {

    "be created from an empty string" in {
      MetricAddress("") mustBe MetricAddress.Root
    }

    "be created from a '/'" in {
      MetricAddress("/") mustBe MetricAddress.Root
    }

    "be created from a string that begin with '/'" in {
      val m = MetricAddress("/foo/bar")
      m.components mustBe List("foo", "bar")
    }

    "be created from a string that doesn't begin with '/'" in {
      val m = MetricAddress("foo/bar")
      m.components mustBe List("foo", "bar")
    }

    "be appended to another MetricAddress" in {
      val addr1 = MetricAddress("/foo")
      val addr2 = MetricAddress("/bar")
      addr1 / addr2 mustBe MetricAddress("/foo/bar")
    }

    "be able to have a string not prefixed with '/' appended to it" in {
      val addr = MetricAddress("/foo")
      addr / "bar" mustBe MetricAddress("/foo/bar")
    }

    "be able to have a string prefixed with '/' appended to it" in {
      val addr = MetricAddress("/foo")
      addr / "/bar" mustBe MetricAddress("/foo/bar")
    }
  }

}
