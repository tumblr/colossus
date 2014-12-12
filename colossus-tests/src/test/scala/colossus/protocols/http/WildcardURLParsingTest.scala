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
          val request = HttpRequest(HttpHead(Get, url, `1.1`, List()), None)
          request match {
            case req @ Get in ROOT => true
            case _ => fail()
          }
      }

      "match a resource url" in {
        val url = "/moose.jpg"
        val request = HttpRequest(HttpHead(Get, url, `1.1`, List()), None)
        request match {
          case req @ Get in ROOT /: rest => rest must be("moose.jpg")
          case _ => fail("Failed to match url")
        }
      }

      "match a complex resource url" in {
        val url = "/bronx/zoo/moose.jpg"
        val request = HttpRequest(HttpHead(Get, url, `1.1`, List()), None)
        request match {
          case req @ Get in base /: city /: place /: animal  =>
            base must be("/")
            city must be("bronx")
            place must be("zoo")
            animal must be("moose.jpg")
          case _ => fail("Failed to match complex url")
        }
      }

      "partially match a url" in {
        val url = "/bronx/zoo/moose.jpg"
        val request = HttpRequest(HttpHead(Get, url, `1.1`, List()), None)
        request match {
          case req @ Get in ROOT /: city /: placeAndAnimal => placeAndAnimal must be("zoo/moose.jpg")
          case _ => fail("Failed to match complex url")
        }
      }
    }
}
