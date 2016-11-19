package colossus.metrics

import org.scalatest._

class CollectionMapSpec extends WordSpec with MustMatchers with BeforeAndAfterAll{

  "CollectionMap" must {

    "increment a non-existing value" in {
      val c = new CollectionMap[String]
      c.get("foo") must equal(None)
      c.increment("foo")
      c.get("foo") must equal(Some(1))
    }

    "increment different keys" in {
      val c = new CollectionMap[String]
      c.get("foo") must equal(None)
      c.get("bar") must equal(None)
      c.increment("foo")
      c.increment("bar")
      c.increment("bar")
      c.get("foo") must equal(Some(1))
      c.get("bar") must equal(Some(2))
    }

    "set a non-existing value" in {
      val c = new CollectionMap[String]
      c.get("foo") must equal(None)
      c.set("foo", 234)
      c.get("foo") must equal(Some(234))
    }

    "snapshot" in {
      val c = new CollectionMap[String]
      c.set("foo", 1)
      c.set("bar", 3)
      c.snapshot(false, false) must equal(Map("foo" -> 1, "bar" -> 3))
      //do again to ensure no reset
      c.snapshot(false, false) must equal(Map("foo" -> 1, "bar" -> 3))
    }

    "snapshot (with reset)" in {
      val c = new CollectionMap[String]
      c.set("foo", 1)
      c.set("bar", 3)
      c.snapshot(false, true) must equal(Map("foo" -> 1, "bar" -> 3))
      c.snapshot(false, true) must equal(Map("foo" -> 0, "bar" -> 0))
    }

    "snapshot (with prune empty)" in {
      val c = new CollectionMap[String]
      c.set("foo", 1)
      c.set("bar", 3)
      c.snapshot(false, true) must equal(Map("foo" -> 1, "bar" -> 3))
      c.snapshot(true, true) must equal(Map())
    }

  }

}

