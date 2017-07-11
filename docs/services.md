---
layout: page
title:  "Services"
categories: docs
---

A service receives requests and returns responses. Colossus has built in support for http, memcache, redis and telnet.

## Http

A http service will take the following form:

{% highlight scala %}
import colossus.IOSystem
import colossus.core.{Initializer, Server, ServerRef}
import colossus.protocols.http.HttpMethod._
import colossus.protocols.http.UrlParsing._
import colossus.protocols.http._
import colossus.service.ServiceConfig
import colossus.service.Callback.Implicits._

object HttpExample {

  def start(port: Int)(implicit system: IOSystem): ServerRef = {
    Server.start("http-example", port){implicit worker => new Initializer(worker) {

      def onConnect = context => new HttpService(ServiceConfig.Default, context){
        def handle = {
          case request @ Get on Root / "hello" => {
            request.ok("hello")
          }
        }
      }
    }}
  }
}
{% endhighlight %}

`ok` is a helper method on `HttpRequest` that returns a `HttpResponse`. The implicit 
`colossus.service.Callback.Implicits.objectToSuccessfulCallback` then turns the response into a `Callback[HttpResponse]`.

The following helper method are available, which will default the content type to text and return the corresponding 
http code.

+ ok
+ notFound
+ error
+ badRequest
+ unauthorized
+ forbidden

There are several different ways to set headers on the response.

{% highlight scala %}
request.ok("hello").withHeader("header-name", "header-value")
request.ok("hello", HttpHeaders(HttpHeader("header-name", "header-value")))
request.ok("hello", HttpHeaders(HttpHeader(HttpHeaders.CookieHeader, "header-value")))
{% endhighlight %}

Setting the content type or using a different http code involves a bit more work as the above helper methods can't be 
used. Instead use the `respond` method.

{% highlight scala %}
request.respond(HttpCodes.CONFLICT, HttpBody("""{"name":"value"}""").withContentType(ContentType.ApplicationJson))
{% endhighlight %}

On the incoming request, body and content type are on the `HttpBody` and headers and parameters are on the 
`HttpHead`.

{% highlight scala %}
request.body.bytes.utf8String
request.body.contentType
request.head.headers
request.head.parameters.getFirst("key")
{% endhighlight %}