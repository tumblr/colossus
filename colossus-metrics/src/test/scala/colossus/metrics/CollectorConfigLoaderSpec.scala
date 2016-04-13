package colossus.metrics

import com.typesafe.config.ConfigFactory
import org.scalatest.{MustMatchers, WordSpec}

class CollectorConfigLoaderSpec extends WordSpec with MustMatchers with CollectorConfigLoader{

  "CollectorConfigLoader" must {
    "load a sequence of paths, omitting non existent paths" in {
      val configValues =
        """
          |a{
          |  foo = "aFoo"
          |}
          |b{
          |  foo = "bFoo"
          |  bar = "bBar"
          |}
          |c{
          |  foo = "cFoo"
          |  bar = "cBar"
          |  baz = "cBaz"
          |}
        """.stripMargin

      val c = ConfigFactory.parseString(configValues)
      val result = resolveConfig(c, "a", "b", "c", "d")
      result.getString("foo") mustBe "aFoo"
      result.getString("bar") mustBe "bBar"
      result.getString("baz") mustBe "cBaz"
    }
  }
}
