package colossus.protocols.http

import org.scalatest.{MustMatchers, WordSpec}

/**
 * Created by zoe on 12/9/14.
 */
class WildcardURLParsingTest extends WordSpec with MustMatchers {
  import colossus.protocols.http.HttpMethod._
  import colossus.protocols.http.HttpVersion._
  import colossus.protocols.http.UrlParsing.Strings._
    "The wildcard url parser" should {
      "match a root url" in {
          val url = ""
          val request = HttpRequest(HttpRequestHead(Get, url, `1.1`, HttpHeaders()), HttpBody.NoBody)
          request match {
            case req @ Get on Root => {}
            case _ => fail()
          }
      }

      "match a resource url" in {
        val url = "/moose.jpg"
        val request = HttpRequest(HttpRequestHead(Get, url, `1.1`, HttpHeaders()), HttpBody.NoBody)
        request match {
          case req @ Get on Root /: rest => rest must be("moose.jpg")
          case _ => fail("Failed to match url")
        }
      }

      "match a complex resource url" in {
        val url = "/bronx/zoo/moose.jpg"
        val request = HttpRequest(HttpRequestHead(Get, url, `1.1`, HttpHeaders()), HttpBody.NoBody)
        request match {
          case req @ Get on base /: city /: place /: animal  =>
            base must be("/")
            city must be("bronx")
            place must be("zoo")
            animal must be("moose.jpg")
          case _ => fail("Failed to match complex url")
        }
      }

      "partially match a url" in {
        val url = "/bronx/zoo/moose.jpg"
        val request = HttpRequest(HttpRequestHead(Get, url, `1.1`, HttpHeaders()), HttpBody.NoBody)
        request match {
          case req @ Get on Root /: city /: placeAndAnimal => placeAndAnimal must be("zoo/moose.jpg")
          case _ => fail("Failed to match complex url")
        }
      }
    }
}
