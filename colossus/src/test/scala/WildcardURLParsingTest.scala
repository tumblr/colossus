import colossus.protocols.http.{HttpHead, HttpRequest}
import org.scalatest.{MustMatchers, WordSpec}

/**
 * Created by zoe on 12/9/14.
 */
class WildcardURLParsingTest extends WordSpec with MustMatchers {
  import colossus.protocols.http.HttpMethod._
  import colossus.protocols.http.HttpVersion._
  import colossus.protocols.http.UrlParsing._
    "The wildcard url parser" should {
      "match a root url" in {
          val url = """http://test.com"""
          val request = HttpRequest(HttpHead(Get, url, `1.1`, List()), None)
          request match {
            case req @ Get in Root => true
            case _ => fail()
          }
      }
    }
}
