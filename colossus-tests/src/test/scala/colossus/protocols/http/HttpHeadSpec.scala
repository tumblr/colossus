package colossus.protocols.http


import org.scalatest._


class HttpRequestHeadSuite extends WordSpec with MustMatchers{
  
  import HttpHeader.Conversions._

  "Cookie" must {
    "parse cookie with equal sign in value" in {
      val header = "set-cookie: foo=bar=baz"
      //just don't throw an exception
      val c = Cookie.parseHeader(header)
      c.size must equal(1)
      c.head.value must equal("bar=baz")
    }
  }

  "HttpHeaders" must {

    "decoded match encoded" in {
      val d = new DecodedHeader("foo", "bar")
      val e = d.toEncodedHeader
      e must equal(d)
    }

    "be equal" in {
      HttpHeaders(HttpHeader("a", "b"), HttpHeader("b", "c")) must equal(HttpHeaders(HttpHeader("b", "c"), HttpHeader("a", "b")))
    }
  }
  

  "HttpRequestHead" must {
    "correctly parse a cookie" in {
      val head = HttpRequestHead(HttpMethod.Get, "/foo", HttpVersion.`1.1`, List(("cookie" -> "foo=bar")))
      val expected = List(
        Cookie("foo", "bar", None)
      )
      head.cookies must equal(expected)
    }
    "correctly parse a cookie with whitespace around the equals" in {
      val head = HttpRequestHead(HttpMethod.Get, "/foo", HttpVersion.`1.1`, List(("cookie" -> "foo = bar")))
      val expected = List(
        Cookie("foo", "bar", None)
      )
      head.cookies must equal(expected)
    }
    "parse two cookies in the same header line" in {
      val head = HttpRequestHead(HttpMethod.Get, "/foo", HttpVersion.`1.1`, List(("cookie" -> "foo=bar ; bar=baz")))
      val expected = List(
        Cookie("foo", "bar", None),
        Cookie("bar", "baz", None)
      )
      head.cookies must equal(expected)
    }
    "parse cookies with expiration" in {
      val head = HttpRequestHead(HttpMethod.Get, "/foo", HttpVersion.`1.1`, List(("cookie" -> "foo=bar ; bar=baz ; Expires=Wed, 09 Jun 2021 10:18:14 GMT")))
      val exp = Cookie.parseCookieExpiration("Wed, 09 Jun 2021 10:18:14 GMT")
      val expected = List(
        Cookie("foo", "bar", Some(exp)),
        Cookie("bar", "baz", Some(exp))
      )
      head.cookies must equal(expected)
    }
    "parse cookie with optional value" in {
      val head = HttpRequestHead(HttpMethod.Get, "/foo", HttpVersion.`1.1`, List(("cookie" -> "foo=bar ; bar")))
      val expected = List(
        Cookie("foo", "bar", None),
        Cookie("bar", "true", None)
      )
      head.cookies must equal(expected)
    }

    "parse query string parameter with no value" in {
      val head = HttpRequestHead(HttpMethod.Get, "/foo?a=&b=c", HttpVersion.`1.1`, Nil)
      head.parameters("a") must equal("")

      val head2 = HttpRequestHead(HttpMethod.Get, "/foo?a&b=c", HttpVersion.`1.1`, Nil)
      head2.parameters("a") must equal("")
    }

    "correctly handle = in query string parameter values" in {
      val head = HttpRequestHead(HttpMethod.Get, "/foo?a=b&b=c=d&e=f=", HttpVersion.`1.1`, Nil)
      head.parameters("a") must equal("b")
      head.parameters("b") must equal("c=d")
      head.parameters("e") must equal("f=")
    }

  }

  "DateHeader" must {
    "generate a correct date" in {
      new String((new DateHeader(123)).encoded(time = 1234567)) must equal("Date: Wed, Dec 31 1969 19:12:34 EST\r\n")
    }
    "only generate a new date when more than a second past the last generated date" in {
      val d = new DateHeader(123)
      new String(d.encoded(time = 1234567)) must equal("Date: Wed, Dec 31 1969 19:12:34 EST\r\n")
      new String(d.encoded(time = 1235566)) must equal("Date: Wed, Dec 31 1969 19:12:34 EST\r\n")
      new String(d.encoded(time = 1235567)) must equal("Date: Wed, Dec 31 1969 19:12:35 EST\r\n")
    }
  }


}

