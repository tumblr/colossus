package colossus
package protocols.http

import core._

import org.scalatest._

import akka.util.ByteString

import parsing._

object Broke extends Tag("broke")

class HttpParserSuite extends WordSpec with MustMatchers{

  def requestParser = HttpRequestParser()
  import HttpHeader.Conversions._
  import HttpBody._

  "http request parser" must {
    "parse a basic request" in {
      val req = "GET /hello/world HTTP/1.1\r\nHost: api.foo.bar:444\r\nAccept: */*\r\nAuthorization: Basic XXX\r\nAccept-Encoding: gzip, deflate\r\n\r\n"
      val parser = requestParser

      val expected = HttpRequest(HttpRequestHead(
        method = HttpMethod.Get,
        url = "/hello/world",
        version = HttpVersion.`1.1`,
        headers = List(
          "host" ->  "api.foo.bar:444",
          "accept" -> "*/*",
          "authorization" -> "Basic XXX",
          "accept-encoding" -> "gzip, deflate"
        )
      ), HttpBody.NoBody)

      parser.parse(DataBuffer(ByteString(req))).toList must equal(List(expected))
    }

    "correctly handle fragments" in {
      val req = "GET /hello/world HTTP/1.1\r\nHost: api.foo.bar:444\r\nAccept: */*\r\nAuthorization: Basic XXX\r\nAccept-Encoding: gzip, deflate\r\n\r\n"
      val parser = requestParser

      val expected = HttpRequest(HttpRequestHead(
        method = HttpMethod.Get,
        url = "/hello/world",
        version = HttpVersion.`1.1`,
        headers = List(
          "host" ->  "api.foo.bar:444",
          "accept" -> "*/*",
          "authorization" -> "Basic XXX",
          "accept-encoding" -> "gzip, deflate"
        )
      ), HttpBody.NoBody)

      (0 until req.length).foreach{splitIndex =>
        val p1 = req.take(splitIndex)
        val p2 = req.drop(splitIndex)
        try {
          parser.parse(DataBuffer(ByteString(p1))).toList must equal(Nil)
          parser.parse(DataBuffer(ByteString(p2))).toList must equal(List(expected))
        } catch {
          case t: Throwable => throw new Exception(s"Failed with splitIndex $splitIndex: '$p1' : '$p2' : $t", t)
        }
      }

    }

    "parse request with no headers" in {
      val req = "GET /hello HTTP/1.1\r\n\r\n"
      val parser = requestParser
      val expected = HttpRequest(HttpRequestHead(method = HttpMethod.Get, url="/hello", version = HttpVersion.`1.1`, headers = Nil), HttpBody.NoBody)
      parser.parse(DataBuffer(ByteString(req))) must equal (Some(expected))
    }

    "parse request with content" in {
      val body = ByteString("HELLO I AM A BODY")
      val len = body.size
      val req = s"POST /hello/world HTTP/1.1\r\nHost: api.foo.bar:444\r\nAccept: */*\r\nContent-Length: $len\r\n\r\n${body.utf8String}"
      val expected = HttpRequest(HttpRequestHead(
        method = HttpMethod.Post,
        url = "/hello/world",
        version = HttpVersion.`1.1`,
        headers = List(
          "host" -> "api.foo.bar:444",
          "accept" ->  "*/*",
          "content-length" -> len.toString
        )
      ), HttpBody(body))

      val parser = requestParser

      parser.parse(DataBuffer(ByteString(req))).toList must equal(List(expected))
    }

    "handle request with content-length set to 0" in {
      val req = s"POST /hello/world HTTP/1.1\r\nHost: api.foo.bar:444\r\nAccept: */*\r\nContent-Length: 0\r\n\r\n"
      val expected = HttpRequest(HttpRequestHead(
        method = HttpMethod.Post,
        url = "/hello/world",
        version = HttpVersion.`1.1`,
        headers = List(
          "host" -> "api.foo.bar:444",
          "accept" -> "*/*",
          "content-length" -> "0"
        )
      ), HttpBody.NoBody)

      val parser = requestParser

      parser.parse(DataBuffer(ByteString(req))).toList must equal(List(expected))

    }

    "properly reset request size tracking on new request" in {
      val req1 = s"POST /hello/world HTTP/1.1\r\nHost: api.foo.bar:444\r\nAccept: */*\r\nContent-Length: 0\r\n\r\n"
      val req2 = s"POST /hello/world HTTP/1.1\r\nHost: api.foo.bar:444\r\nAccept: */*\r\nContent-Length: 0\r\n\r\n"
      val requests = req1 + req2
      val parser = requestParser//(req1.size.bytes)
      parser.parse(DataBuffer(ByteString(req1)))
      parser.parse(DataBuffer(ByteString(req2)))
      true must equal(true) //test succeeds unless exception is thrown

    }

    "parse request with authorization header" taggedAs(Broke) in {
      val auth_info = "Basic YmI6Y29vbHBhc3N3b3JkYnJvNDI="// + Base64.encodeBase64String(StringUtils.getBytesUtf8("bb:coolpasswordbro42"))
      val url = "/foo"
      val request = HttpRequest(HttpRequestHead(version = HttpVersion.`1.1`, method = HttpMethod.Get, url = url, headers = List("authorization" -> auth_info)), HttpBody.NoBody)
      val d = new DynamicOutBuffer(100, false)
      request.encode(d)
      val parser = requestParser
      parser.parse(d.data).isEmpty must equal(false)
    }

    //the current parser hangs instead of throwing an exception.  Need to
    //decide if that is acceptable behavior

    "reject request missing version" in {
      val req = s"GET /oops\r\n\r\n"
      val parser = requestParser
      intercept[ParseException]{
        println(parser.parse(DataBuffer(ByteString(req))))
      }
    }
    "reject request missing method" in {
      val req = s"/oops Http/1.1\r\n\r\n"
      val parser = requestParser
      intercept[ParseException]{
        println(parser.parse(DataBuffer(ByteString(req))))
      }
    }
    "reject request missing path" in {
      val req = s"GET Http/1.1\r\n\r\n"
      val parser = requestParser
      intercept[ParseException]{
        println(parser.parse(DataBuffer(ByteString(req))))
      }
    }
    "reject empty request" in {
      val req = s"\r\n\r\n"
      val parser = requestParser
      intercept[ParseException]{
        println(parser.parse(DataBuffer(ByteString(req))))
      }
    }
    "reject request with random newline in header" in {
      val req = s"GET /foo Http/1.1\r\nwat\r\nsomething: value\r\n\r\n"
      val parser = requestParser
      intercept[ParseException]{
        println(parser.parse(DataBuffer(ByteString(req))))
      }
    }
    "reject request with space in path" ignore {
      val req = s"GET /foo?something=hello world Http/1.1\r\nsomething: value\r\n\r\n"
      val parser = requestParser
      intercept[ParseException]{
        println(parser.parse(DataBuffer(ByteString(req))))
      }
    }
    "reject request with bad version" in {
      val req = s"GET /foo Http/3.14\r\nsomething:value\r\n\r\n"
      val parser = requestParser
      intercept[ParseException]{
        println(parser.parse(DataBuffer(ByteString(req))))
      }
    }

    "parse a generated request" in {
      val req = HttpRequest(HttpRequestHead(version = HttpVersion.`1.1`, method = HttpMethod.Get, url = "/foo", headers = Nil), HttpBody.NoBody)
      val d = new DynamicOutBuffer(100, false)
      req.encode(d)
      val data = d.data
      val parser = requestParser
      parser.parse(data) must equal(Some(req))
      data.remaining must equal(0)
    }

    "parse a request with chunked transfer encoding" in {
      val req = s"GET /foo HTTP/1.1\r\nsomething:value\r\ntransfer-encoding: chunked\r\n\r\n3\r\nfoo\r\ne\r\n123456789abcde\r\n0\r\n\r\n"
      val data = DataBuffer(ByteString(req))
      val parser = requestParser
      val parsed = parser.parse(data).get
      parsed.body must equal(HttpBody(ByteString("foo123456789abcde")))
      data.remaining must equal(0)
    }




  }

  "HttpMethod" must {
    "parse itself" in {
      HttpMethod.methods.foreach{method =>
        HttpMethod(method.bytes) must equal(method)
      }
    }

    "reject some lookalike methods" in {
      def m(s: String) = s.getBytes("UTF-8")

      intercept[ParseException] {
        HttpMethod(m("GOT"))
      }
      intercept[ParseException] {
        HttpMethod(m("PET"))
      }
      intercept[ParseException] {
        HttpMethod(m("GOODMORNING"))
      }
      intercept[ParseException] {
        HttpMethod(m("OPTOIN"))
      }
      intercept[ParseException] {
        HttpMethod(m("PITCH"))
      }
      intercept[ParseException] {
        HttpMethod(m("ZZZZ"))
      }
    }
  }



  "HttpRequestHead parameter parsing" must {

    def h(url: String) = HttpRequestHead(HttpMethod.Get, url, HttpVersion.`1.1`, Nil).parameters
    def p(params: (String, String)*) = QueryParameters(params.toSeq)

    "parse a basic url" in {
      h("/hello/blah.php?foo=bar&baz=moo") must equal(p("foo" -> "bar" , "baz" -> "moo"))
    }

    "decode url-encoded stuff" in {
      h("/x?he%26llo=wo%3F%3Dd") must equal(p("he&llo" -> "wo?=d"))
    }

    "handle terminating &" in {
      h("/x?a=b&") must equal(p("a" -> "b"))
    }

    "handle empty value" in {
      h("/x?a=&b=c") must equal(p("a" -> "", "b" -> "c"))
    }

    "getFirst" in {
      val p = h("/x?a=111&a=222&b=333")
      p.getFirst("a") must equal(Some("111"))
      p.getFirst("b") must equal(Some("333"))
      p.getFirst("c") must equal(None)
    }

    "getAll" in {
      val p = h("/x?a=111&a=222&b=333")
      p.getAll("a") must equal(Seq("111", "222"))
      p.getAll("b") must equal(Seq("333"))
      p.getAll("c") must equal(Seq())
    }

    "contains" in {
      val p = h("/x?a=111&a=222&b=333")
      p.contains("a") must equal(true)
      p.contains("b") must equal(true)
      p.contains("c") must equal(false)
    }


  }

  "Http Url Matcher" must {
    import UrlParsing._
    import HttpMethod._
    import HttpVersion._

    "parse a url" in {
      Url.parse("/foo/bar/baz") must equal (ParsedUrl("baz" :: "bar" :: "foo" :: Nil))
    }
    "parse a url with integer" in {
      Url.parse("/foo/3/baz") must equal (ParsedUrl("baz" :: "3" :: "foo" :: Nil))
    }

    "match url with one component" in {
      val u = Url.parse("/a")
      val r = u match {
        case ParsedUrl(List("a")) => true
        case _ => false
      }
      r must equal(true)
    }



    "match url" in {
      val u = Url.parse("/a/b/c")
      val r = u match {
        case a / b / c => true
        case _ => false
      }
      r must equal(true)
    }

    "match on http request" in {
      val h = HttpRequest(HttpRequestHead(Get, "/a/b/c", `1.1`, Nil), HttpBody.NoBody)
      val res: Boolean = h match {
        case Get on Root / "a" / "b" / "c" => true
        case _ => false
      }
      res must equal(true)
    }
    "match on http request with one route" in {
      val h = HttpRequest(HttpRequestHead(Get, "/a", `1.1`, Nil), HttpBody.NoBody)
      val res: Boolean = h match {
        case Get on Root / "a" => true
        case _ => false
      }
      res must equal(true)
    }

    "match http request with integer" in {
      val h = HttpRequest(HttpRequestHead(Get, "/foo/bar/3", `1.1`, Nil), HttpBody.NoBody)
      val res: Int = h match {
        case Get on Root / "foo" / "bar" / Integer(x) => x
        case _ => 0
      }
      res must equal(3)
    }



    "match http request with long" in {
      val h = HttpRequest(HttpRequestHead(Get, "/foo/bar/17592186044416", `1.1`, Nil), HttpBody.NoBody)
      val res: Long = h match {
        case Get on Root / "foo" / "bar" / Long(x) => x
        case _ => 0
      }
      res must equal(17592186044416L)
    }

    "match root url" in {
      val h = HttpRequest(HttpRequestHead(Get, "/", `1.1`, Nil), HttpBody.NoBody)
      val res: Boolean = h match {
        case Get on Root => true
        case _ => false
      }
      res must equal(true)
    }

    "parse requests with no request parameters" in {
      val req = HttpRequest(HttpRequestHead(HttpMethod.Get, "a/path/with/1", HttpVersion.`1.1`, Nil), HttpBody.NoBody)
      req match {
        case r @ Get on Root / "a" / "path" / "with" / Integer(1) =>
        case _ => throw new Exception(s"$req failed to parse correctly")
      }
    }

    "parse requests with request parameters" in {
      val req = HttpRequest(HttpRequestHead(HttpMethod.Get, "a/path/with/1?a=bla&b=2", HttpVersion.`1.1`, Nil), HttpBody.NoBody)
      req match {
        case r @ Get on Root / "a" / "path" / "with" / Integer(1) =>
        case _ => throw new Exception(s"$req failed to parse correctly")
      }
    }


  }


}
