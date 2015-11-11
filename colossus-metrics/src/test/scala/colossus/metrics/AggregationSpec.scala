package colossus.metrics

import MetricAddress._
import org.scalatest._

import MetricValues._

class AggregationSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  "MetricSelection" must {
    "basic exact match" in {
      val s = Root / "foo" / "bar" / "baz"
      s.matches(s) must equal (true)
      s.matches(Root / "foo" / "bar") must equal (false)
      s.matches(Root / "foo" / "bar" / "baz" / "mooo") must equal (false)
    }

    "trailing wildcard match" in {
      val m = Root / "foo" / "bar" / "baz"
      val s = Root / "foo" / "*"
      s.matches(m) must equal (true)
      s.matches(Root / "bar" / "baz") must equal(false)
    }

    "inside wildcard match" in {
      val m = Root / "foo" / "bar" / "baz"
      val s = Root / "foo" / "*" / "baz"
      s.matches(m) must equal (true)
      s.matches(Root / "foo" / "asfasdf" / "baz") must equal(true)
      s.matches(Root / "foo" / "bar") must equal(false)
      s.matches(Root / "bar" / "baz") must equal(false)
    }

  }

  "TagSelector" must {
    "match tags" in {
      val t = TagSelector(
        Map("foo" -> List("bar", "baz"))
      )
      t.matches(Map("foo" -> "bar")) must equal (true)
      t.matches(Map("foo" -> "ggg")) must equal (false)
      t.matches(Map("rrr" -> "baz")) must equal (false)
    }
    "match tags with wildcard" in {
      val t = TagSelector(
        Map("foo" -> List("*"), "bar" -> List("baz"))
      )
      t.matches(Map("foo" -> "bar", "bar" -> "baz")) must equal (true)
      t.matches(Map("foo" -> "adfsdf", "bar" -> "baz")) must equal (true)
      t.matches(Map("foo" -> "adfsdf", "bar" -> "zzz")) must equal (false)
      t.matches(Map("bar" -> "baz")) must equal (false)

    }
  }

  "MetricValueFilter" must {
    "only keep desired values" in {
      val v1 = Map("foo" -> "bar") -> SumValue(4L)
      val v2 = Map("foo" -> "bar", "bar" -> "baz") -> SumValue(5L)
      val v3 = Map("foo" -> "moo", "bar" -> "noo") -> SumValue(6L)
      val v4 = TagMap.Empty -> SumValue(8L)
      val values: ValueMap = Map(v1,v2,v3,v4)
      val expected: RawValueMap = Map(v2).toRawValueMap

      val filter = MetricValueFilter(Some(TagSelector(Map("foo" -> Seq("*"), "bar" -> Seq("baz")))), None)

      filter.process(values) must equal (expected)
    }
  }

}

