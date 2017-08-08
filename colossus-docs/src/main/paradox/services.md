# Services

A service receives requests and returns responses. Colossus has built in support for http, memcache, redis and telnet.

## Http

A http service will take the following form:

@@snip [HttpService.scala](../scala/HttpService.scala) { #example }

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

@@snip [HttpService2.scala](../scala/HttpService2.scala) { #example1 }

Setting the content type or using a different http code involves a bit more work as the above helper methods can't be 
used. Instead use the `respond` method.

@@snip [HttpService2.scala](../scala/HttpService2.scala) { #example2 }

On the incoming request, body and content type are on the `HttpBody` and headers and parameters are on the 
`HttpHead`.

@@snip [HttpService2.scala](../scala/HttpService2.scala) { #example3 }