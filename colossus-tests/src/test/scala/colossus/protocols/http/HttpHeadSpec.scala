package colossus.protocols.http


import org.scalatest._


class HttpHeadSuite extends WordSpec with MustMatchers{

  "Cookie" must {
    "parse cookie with equal sign in value" in {
      val header = "set-cookie: foo=bar=baz"
      //just don't throw an exception
      val c = Cookie.parseHeader(header)
      c.size must equal(1)
      c.head.value must equal("bar=baz")
    }
  }
  

  "HttpHead" must {
    "correctly parse a cookie" in {
      val head = HttpHead(HttpMethod.Get, "/foo", HttpVersion.`1.1`, List(("cookie" -> "foo=bar")))
      val expected = List(
        Cookie("foo", "bar", None)
      )
      head.cookies must equal(expected)
    }
    "correctly parse a cookie with whitespace around the equals" in {
      val head = HttpHead(HttpMethod.Get, "/foo", HttpVersion.`1.1`, List(("cookie" -> "foo = bar")))
      val expected = List(
        Cookie("foo", "bar", None)
      )
      head.cookies must equal(expected)
    }
    "parse two cookies in the same header line" in {
      val head = HttpHead(HttpMethod.Get, "/foo", HttpVersion.`1.1`, List(("cookie" -> "foo=bar ; bar=baz")))
      val expected = List(
        Cookie("foo", "bar", None),
        Cookie("bar", "baz", None)
      )
      head.cookies must equal(expected)
    }
    "parse cookies with expiration" in {
      val head = HttpHead(HttpMethod.Get, "/foo", HttpVersion.`1.1`, List(("cookie" -> "foo=bar ; bar=baz ; Expires=Wed, 09 Jun 2021 10:18:14 GMT")))
      val exp = Cookie.parseCookieExpiration("Wed, 09 Jun 2021 10:18:14 GMT")
      val expected = List(
        Cookie("foo", "bar", Some(exp)),
        Cookie("bar", "baz", Some(exp))
      )
      head.cookies must equal(expected)
    }
    "parse cookie with optional value" in {
      val head = HttpHead(HttpMethod.Get, "/foo", HttpVersion.`1.1`, List(("cookie" -> "foo=bar ; bar")))
      val expected = List(
        Cookie("foo", "bar", None),
        Cookie("bar", "true", None)
      )
      head.cookies must equal(expected)
    }

    "parse query string parameter with no value" in {
      val head = HttpHead(HttpMethod.Get, "/foo?a=&b=c", HttpVersion.`1.1`, Nil)
      head.parameters("a") must equal("")

      val head2 = HttpHead(HttpMethod.Get, "/foo?a&b=c", HttpVersion.`1.1`, Nil)
      head2.parameters("a") must equal("")
    }

    "correctly handle = in query string parameter values" in {
      val head = HttpHead(HttpMethod.Get, "/foo?a=b&b=c=d&e=f=", HttpVersion.`1.1`, Nil)
      head.parameters("a") must equal("b")
      head.parameters("b") must equal("c=d")
      head.parameters("e") must equal("f=")
    }
  }


}

